# Copyright (c) 2010 Apple  Inc. All rights reserved.

# IMPORTANT:  This Apple software is supplied to you by Apple Inc. ("Apple") in
# consideration of your agreement to the following terms, and your use,
# installation, modification or redistribution of this Apple software
# constitutes acceptance of these terms.  If you do not agree with these terms,
# please do not use, install, modify or redistribute this Apple software.

# In consideration of your agreement to abide by the following terms, and subject
# to these terms, Apple grants you a personal, non-exclusive license, under Apple's
# copyrights in this original Apple software (the "Apple Software"), to use,
# reproduce, modify and redistribute the Apple Software, with or without
# modifications, in source and/or binary forms; provided that if you redistribute
# the Apple Software in its entirety and without modifications, you must retain
# this notice and the following text and disclaimers in all such redistributions
# of the Apple Software.  Neither the name, trademarks, service marks or logos of
# Apple Inc. may be used to endorse or promote products derived from the Apple
# Software without specific prior written permission from Apple.  Except as
# expressly stated in this notice, no other rights or licenses, express or implied,
# are granted by Apple herein, including but not limited to any patent rights that
# may be infringed by your derivative works or by other works in which the Apple
# Software may be incorporated.

# The Apple Software is provided by Apple on an "AS IS" basis.  APPLE MAKES NO
# WARRANTIES, EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION THE IMPLIED
# WARRANTIES OF NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE, REGARDING THE APPLE SOFTWARE OR ITS USE AND OPERATION ALONE OR IN
# COMBINATION WITH YOUR PRODUCTS.

# IN NO EVENT SHALL APPLE BE LIABLE FOR ANY SPECIAL, INDIRECT, INCIDENTAL OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
# GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# ARISING IN ANY WAY OUT OF THE USE, REPRODUCTION, MODIFICATION AND/OR DISTRIBUTION
# OF THE APPLE SOFTWARE, HOWEVER CAUSED AND WHETHER UNDER THEORY OF CONTRACT, TORT
# (INCLUDING NEGLIGENCE), STRICT LIABILITY OR OTHERWISE, EVEN IF APPLE HAS BEEN
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import datetime
import os
import re
import logging
import subprocess

LOGGER = logging.getLogger()

TAR_HEADER_SIZE = 512  # exactly one filesystem block.

class SubstringNotFoundException(Exception):
    """
    Exception thrown when a comment character or other tag is not found in a situation where it's required.
    """


class Parser(object):
    """
    Parses an EPF file.

    During initialization, all the file db metadata is stored, and the
    file seek position is set to the beginning of the first data record.
    The Parser object can then be used directly by an Ingester to create
    and populate the table.

    typeMap is a dictionary mapping datatype strings in the file to corresponding
    types for the database being used. The default map is for MySQL.
    """
    commentChar = "#"
    recordDelim = "\x02\n"
    fieldDelim = "\x01"
    primaryKeyTag = "primaryKey:"
    dataTypesTag = "dbTypes:"
    exportModeTag = "exportMode:"
    recordCountTag = "recordsWritten:"

    def __init__(self, filePath, typeMap={"CLOB":"LONGTEXT"}, recordDelim='\x02\n', fieldDelim='\x01'):
        self.dataTypeMap = typeMap
        self.numberTypes = ["INTEGER", "INT", "BIGINT", "TINYINT"]
        self.dateTypes = ["DATE", "DATETIME", "TIME", "TIMESTAMP"]
        self.columnNames = []
        self.primaryKey = []
        self.primaryKeyIndexes = []
        self.dataTypes = []
        self.exportMode = None
        self.dateColumns = [] #fields containing dates need special treatment; we'll cache the indexes here
        self.numberColumns = [] #numeric fields don't accept NULL; we'll cache the indexes here to use later
        self.typeMap = None
        self.recordsExpected = 0
        self.latestRecordNum = 0
        self.commentChar = Parser.commentChar
        self.recordDelim = recordDelim
        self.fieldDelim = fieldDelim

        # self.bzFile = io.open(filePath, mode='rb', buffering=102400) # 100k is bzip's minimum block size
        # self.rawFile = io.BufferedReader(self.bzFile, buffer_size=102400)
        # self.eFile = bz2.open(self.rawFile, 'rb')

        self.process = subprocess.Popen(['bunzip2', '-c', filePath], stdout=subprocess.PIPE, bufsize=16384)
        self.eFile = self.process.stdout
        self.eFile.read(TAR_HEADER_SIZE)  # skip tarfile header

        self.fileSize = os.path.getsize(filePath)

        # An exact record count exists in the last row of the file, but that would involve extracting the entire tarfile
        # first - something we want to avoid. So, instead, we just guess based on the input file size. This is ONLY used
        # to determine the ingestion strategy, not for anything else.
        self.recordsExpected = 499999 if self.fileSize < 100000000 else 500001

        #Extract the column names
        line1 = self.nextRowString(ignoreComments=False)
        self.columnNames = self.splitRow(line1, requiredPrefix=self.commentChar)

        #We'll now grab the rest of the header data, without assuming a particular order
        primStart = self.commentChar+Parser.primaryKeyTag
        dtStart = self.commentChar+Parser.dataTypesTag
        exStart = self.commentChar+Parser.exportModeTag

        #Grab the next 6 lines, which should include all the header comments
        firstRows=[]
        for j in range(10):
            firstRows.append(self.nextRowString(ignoreComments=False))
            firstRows = [aRow for aRow in firstRows if aRow] #strip None rows (possible if the file is < 6 rows)

        #Loop through the rows, extracting the header info
        for aRow in firstRows:
            if aRow.startswith(primStart):
                self.primaryKey = self.splitRow(aRow, requiredPrefix=primStart)
                self.primaryKey = ([] if self.primaryKey == [''] else self.primaryKey)
            elif aRow.startswith(dtStart):
                dts = self.splitRow(aRow, requiredPrefix=dtStart)
                # HACK doing terrible things to make the retail_price column big enough
                self.dataTypes = ['DECIMAL(11,3)' if dt == 'DECIMAL(9,3)' else dt for dt in dts]
            elif aRow.startswith(exStart):
                self.exportMode = self.splitRow(aRow, requiredPrefix=exStart)[0]

        self.fixupDataTypes()

        for pk in self.primaryKey:
            self.primaryKeyIndexes.append(self.columnNames.index(pk))

        #Convert any datatypes to mapped counterparts, and cache indexes of date/time types and number types
        for j in range(len(self.dataTypes)):
            dType = self.dataTypes[j]
            if dType in self.dataTypeMap:
                self.dataTypes[j] = self.dataTypeMap[dType]
            if dType in self.dateTypes:
                self.dateColumns.append(j)
            if dType in self.numberTypes:
                self.numberColumns.append(j)
        #Build a dictionary of column names to data types
        self.typeMap = dict(zip(self.columnNames, self.dataTypes))

        # used in nextRecord
        self.nonNumberMatch = re.compile(r'[^0-9.-]+')


    def fixupDataTypes(self):
        """Fixup data types for Apple bug."""
        for index, (column, dbType) in enumerate(zip(self.columnNames, self.dataTypes)):
            if column == 'export_date':
                self.dataTypes[index] = 'BIGINT'
            elif column.endswith('_date'):
                self.dataTypes[index] = 'DATETIME'
            elif column.endswith('_id') and dbType not in ('INTEGER', 'BIGINT'):
                self.dataTypes[index] = 'BIGINT'
            elif column.startswith('is_'):
                self.dataTypes[index] = 'BOOLEAN'


    def setSeekPos(self, pos=0):
        """
        Sets the underlying file's seek position.

        This is useful for resuming a partial ingest that was interrupted for some reason.
        """
        self.eFile.seek(pos)


    def getSeekPos(self):
        """
        Gets the underlying file's seek position.
        """
        return self.eFile.tell()

    seekPos = property(fget=getSeekPos, fset=setSeekPos, doc="Seek position of the underlying file")


    def seekToRecord(self, recordNum):
        """
        Set the seek position to the beginning of the recordNumth record.

        Seeks to the beginning of the file if recordNum <=0,
        or the end if it's greater than the number of records.

        N.B. with tbz streams, "0" is actually at 512.
        """
        if (recordNum <= 0):
            return
        if self.seekPos != TAR_HEADER_SIZE:
            self.seekPos = TAR_HEADER_SIZE
        self.latestRecordNum = 0
        for j in range(recordNum):
            self.advanceToNextRecord()


    def nextRowString(self, ignoreComments=True):
        """
        Returns (as a string) the next row of data (as delimited by self.recordDelim),
        ignoring comments if ignoreComments is True.

        Leaves the delimiters in place.

        Unfortunately Python doesn't allow line-based reading with user-supplied line separators
        (http://bugs.python.org/issue1152248), so we use normal line reading and then concatenate
        when we hit 0x02.
        """
        lst = []
        isFirstLine = True
        while True:
            ln = self.eFile.readline()
            if not ln or ln == b'' or ln[0] == 0: #end of file - skipping zero-fill at the end of tarfile
                break
            if chr(ln[0]) == self.commentChar and isFirstLine and ignoreComments: #comment
                continue
            lst.append(ln)
            if isFirstLine:
                isFirstLine = False
            if ln.endswith(bytes(self.recordDelim, 'utf-8')): #last textual line of this record
                break
        if (len(lst) == 0):
            return None
        else:
            rowString = b''.join(lst) #concatenate the lines into a single string, which is the full content of the row
            return str(rowString, 'utf-8')


    def advanceToNextRecord(self):
        """
        Performs essentially the same task as nextRowString, but without constructing or returning anything.
        This allows much faster access to a record in the middle of the file.
        """
        while (True):
            ln = str(self.eFile.readline(), "utf-8")
            if (not ln): #end of file
                return
            if (ln.find(self.commentChar) == 0): #comment; always skip
                continue
            if (ln.find(self.recordDelim) != -1): #last textual line of this record
                break
        self.latestRecordNum += 1


    def splitRow(self, rowString, requiredPrefix=None):
        """
        Given rowString, strips requiredPrefix and self.recordDelim,
        then splits on self.fieldDelim, returning the resulting list.

        If requiredPrefix is not present in the row, throws a SubstringNotFound exception
        """
        if (requiredPrefix):
            if not rowString.startswith(requiredPrefix):
                expl = "Required prefix '%s' was not found in '%s'" % (requiredPrefix, rowString)
                raise SubstringNotFoundException(expl)
            rowString = rowString.partition(requiredPrefix)[2]
        str = rowString.partition(self.recordDelim)[0]
        return str.split(self.fieldDelim)


    def nextRecord(self):
        """
        Returns the next row of data as a list, or None if we're out of data.
        """
        rowString = self.nextRowString()
        if (rowString):
            self.latestRecordNum += 1 #update the record counter
            rec = self.splitRow(rowString)
            rec = rec[:len(self.columnNames)] #if there are more data records than column names,
            #trim any surplus records via a slice

            #replace empty strings with None
            for i in range(len(rec)):
                val = rec[i]
                rec[i] = (None if val == "" and i not in self.primaryKeyIndexes else val)

            #massage dates into MySQL-compatible format.
            #most date values look like '2009 06 21'; some are '2005-09-06-00:00:00-Etc/GMT'
            #there are also some cases where there's only a year; we'll pad it out with a bogus month/day

            for j in self.dateColumns:
                if rec[j]:
                    rec[j] = rec[j].strip()
                    if len(rec[j]) > 3:
                        if rec[j][2] in ' -':
                            # found a 2-digit year
                            rec[j] = f'20{rec[j]}'
                            if int(rec[j][:4]) > datetime.date.today().year:
                                rec[j] = f'19{rec[j][2:]}'

                        elif rec[j][1] in ' -':
                            # found a 1-digit year
                            rec[j] = f'200{rec[j]}'
                            if int(rec[j][:4]) > datetime.date.today().year:
                                rec[j] = f'199{rec[j][3:]}'

                    rec[j] = rec[j][:19].replace("-", " ") # cut the timezone info

                    if len(rec[j]) == 4:
                        rec[j] = f"{rec[j]}-01-01"


            for j in self.numberColumns:
                if rec[j] and not rec[j][0].isdigit():
                    # we've seen at least one integer field in a file with square brackets around it. Remove.
                    # r'[^0-9.-]'
                    rec[j] = self.nonNumberMatch.sub('', rec[j])
                    if rec[j] == '':
                        # Have seen at least one instance of the text "<UnknownKeyException>" in a column.
                        # lost cause - just skip the record entirely.
                        LOGGER.warning("Skipping record %i because it is malformed", self.latestRecordNum)
                        return self.nextRecord()

            return rec
        else:
            return None


    def nextRecords(self, maxNum=100):
        """
        Returns the next maxNum records (or fewer if EOF) as a list of lists.
        """
        records = []
        for j in range(maxNum):
            lst = self.nextRecord()
            if (not lst):
                break
            records.append(lst)
        return records


    def nextRecordDict(self):
        """
        Returns the next row of data as a dictionary, keyed by the column names.
        """
        vals = self.nextRecord()
        if (not vals):
            return None
        else:
            keys = self.columnNames
            return dict(zip(keys, vals))

