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
import select

import EPFParser
import pymysql as MySQLdb
try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass
import psycopg2
import os
import datetime
import warnings
import logging

DATETIME_FORMAT = "%y-%m-%d %H:%M:%S"

LOGGER = logging.getLogger()


class Ingester(object):
    """
    Used to ingest an EPF file into a MySQL database.
    """
    #MySQLdb turns MySQL warnings into python warnings, whose behavior is somewhat arcane
    #(as compared with python exceptions.
    #By default, turn all warnings into exceptions
    warnings.filterwarnings('error')
    #Supress warnings that occur when we do a 'DROP TABLE IF EXISTS'; we expect these,
    #so there's no point in cluttering up the output with them.
    warnings.filterwarnings('ignore', 'Unknown table.*')

    def __init__(self,
            filePath,
            tablePrefix=None,
            dbHost='localhost',
            dbUser='epfimporter',
            dbPassword='epf123',
            dbName='epf',
            dbType='mysql',
            recordDelim='\x02\n',
            fieldDelim='\x01'):
        """
        """
        self.filePath = filePath
        self.fileName = os.path.basename(filePath)
        self.tableName = self.fileName.replace("-", "_") #hyphens aren't allowed in table names
        self.tableName = self.tableName.split(".")[0]

        pref = ""
        if tablePrefix:
            # add _ separator to prefix unless the prefix is a schema or empty
            pref = ("%s_" % tablePrefix if len(tablePrefix) and tablePrefix[-1] != "." else tablePrefix)

        self.tableSchema = None
        if "." in pref:
            # looks like there's a schema.
            self.tableSchema, pref = pref.split(".")

        self.tableName = (pref + self.tableName)
        self.tmpTableName = self.tableName + "_tmp"
        self.incTableName = self.tableName + "_inc" #used during incremental ingests
        self.unionTableName = self.tableName + "_un" #used during incremental ingests
        self.dbHost = dbHost
        self.dbUser = dbUser
        self.dbPassword = dbPassword
        self.dbName = dbName
        self.dbType = dbType
        self.isPostgresql = (dbType == "postgresql")
        self.isMysql = (dbType == "mysql")
        self.lastRecordIngested = -1
        if self.isPostgresql:
            self.parser = EPFParser.Parser(filePath, typeMap={"VARCHAR(1000)":"TEXT", "VARCHAR(4000)":"TEXT", "CLOB":"LONGTEXT", "DATETIME":"TIMESTAMP", "LONGTEXT":"TEXT"}, recordDelim=recordDelim, fieldDelim=fieldDelim)
        else:
            self.parser = EPFParser.Parser(filePath, recordDelim=recordDelim, fieldDelim=fieldDelim)
        self.startTime = None
        self.endTime = None
        self.abortTime = None
        self.didAbort = False
        self.statusDict = {}
        self.updateStatusDict()
        self.lastRecordCheck = 0
        self.lastTimeCheck = datetime.datetime.now()


    def updateStatusDict(self):
        self.statusDict['fileName'] = self.fileName
        self.statusDict['filePath'] = self.filePath
        self.statusDict['lastRecordIngested'] = self.lastRecordIngested
        self.statusDict['startTime'] = (str(self.startTime) if self.startTime else None)
        self.statusDict['endTime'] = (str(self.endTime) if self.endTime else None)
        self.statusDict['abortTime'] = (str(self.abortTime) if self.abortTime else None)
        self.statusDict['didAbort'] = self.didAbort


    def ingest(self, skipKeyViolators=False):
        """
        Perform a full or incremental ingest, depending on self.parser.exportMode
        """
        if self.parser.exportMode == 'INCREMENTAL':
            self.ingestIncremental(skipKeyViolators=skipKeyViolators)
        else:
            self.ingestFull(skipKeyViolators=skipKeyViolators)
        self.parser.eFile.close()
        self.parser.process.poll()


    def ingestFull(self, skipKeyViolators=False):
        """
        Perform a full ingest of the file at self.filePath.

        This is done as follows:
        1. Create a new table with a temporary name
        2. Populate the new table
        3. Drop the old table and rename the new one
        """
        LOGGER.info("Beginning full ingest of %s (%i bytes)", self.tableName, self.parser.fileSize)
        self.startTime = datetime.datetime.now()
        try:
            self._createTable(self.tmpTableName)
            self._populateTable(self.tmpTableName, skipKeyViolators=skipKeyViolators)
            self._renameAndDrop(self.tmpTableName, self.tableName)
        except (MySQLdb.Error, psycopg2.Error):
            LOGGER.exception("Fatal error encountered while ingesting '%s'", self.filePath)
            LOGGER.error("Last record ingested before failure: %d", self.lastRecordIngested)
            self.abortTime = datetime.datetime.now()
            self.didAbort = True
            self.updateStatusDict()
            raise #re-raise the exception
        #ingest completed
        self.endTime = datetime.datetime.now()
        self.updateStatusDict()
        LOGGER.info("Full ingest of %s took %s", self.tableName, str(self.endTime - self.startTime))


    def ingestFullResume(self, fromRecord=0, skipKeyViolators=False):
        """
        Resume an interrupted full ingest, continuing from fromRecord.
        """
        LOGGER.info("Resuming full ingest of %s (%i bytes)", self.tableName, self.parser.fileSize)
        self.lastRecordIngested = fromRecord - 1
        self.startTime = datetime.datetime.now()
        try:
            self._populateTable(self.tmpTableName, resumeNum=fromRecord, skipKeyViolators=skipKeyViolators)
            self._renameAndDrop(self.tmpTableName, self.tableName)
        except (MySQLdb.Error, psycopg2.Error):
            #LOGGER.error("Error %d: %s", e.args[0], e.args[1])
            LOGGER.error("Error encountered while ingesting '%s'", self.filePath)
            LOGGER.error("Last record ingested before failure: %d", self.lastRecordIngested)
            raise #re-raise the exception
        self.endTime = datetime.datetime.now()
        ts = str(self.endTime - self.startTime)
        LOGGER.info("Resumed full ingest of %s took %s", self.tableName, ts[:len(ts)-4])


    def ingestIncremental(self, fromRecord=0, skipKeyViolators=False):
        """
        Update the table with the data in the file at filePath.

        If the file to ingest has < 500,000 records, we do a simple REPLACE operation
        on the existing table. If it's larger than that, we use the following 3-step process:
        1. Create a temporary table, and populate it exactly as though it were a Full ingest
        2. Perform a SQL query which selects all rows in the old table whose primary keys *don't*
           match those in the new table, unions the result with all rows in the new table, and
           writes the resulting set to another temporary table.
        3. Swap out the old table for the new one via a rename (same as for Full ingests)
        This proves to be much faster for large files.
        """
        if not (self.tableExists(self.tableName)):
            #The table doesn't exist in the db; this can happen if the full ingest
            #in which the table was added wasn't performed.
            LOGGER.warn("Table '%s' does not exist in the database; skipping", self.tableName)
        else:
            tableColCount = self.columnCount()
            fileColCount = len(self.parser.columnNames)
            assert (tableColCount <= fileColCount) #It's possible for the existing table
            #to have fewer columns than the file we're importing, but it should never have more.

            if fileColCount > tableColCount: #file has "extra" columns
                LOGGER.warn("File contains additional columns not in the existing table. These will not be imported.")
                self.parser.columnNames = self.parser.columnNames[:tableColCount] #trim the columnNames
                # to equal those in the existing table. This will result in the returned records
                # also being sliced.
            s = ("Resuming" if fromRecord else "Beginning")
            LOGGER.info("%s incremental ingest of %s (%i bytes)", s, self.tableName, self.parser.fileSize)
            self.startTime = datetime.datetime.now()

            #Different ingest techniques are faster depending on the size of the input.
            #If there are a large number of records, it's much faster to do a prune-and-merge technique;
            #for fewer records, it's faster to update the existing table.
            try:
                # XXX: we always update in place. because collection_price REFUSES to be union merged in Postgres
                #      ... takes 3 hours to create the union table before failing. Update in place is always faster.
                if self.isPostgresql or self.parser.recordsExpected < 500000: #update table in place
                    self._populateTable(self.tableName,
                                    resumeNum=fromRecord,
                                    isIncremental=True,
                                    skipKeyViolators=skipKeyViolators)
                else: #Import as full, then merge the proper records into a new table
                    self._createTable(self.incTableName)
                    LOGGER.info("Populating temporary table...")
                    self._populateTable(self.incTableName, skipKeyViolators=skipKeyViolators)
                    LOGGER.info("Creating merged table...")
                    self._createUnionTable()
                    self._dropTable(self.incTableName)
                    LOGGER.info("Applying primary key constraints...")
                    self._applyPrimaryKeyConstraints(self.unionTableName)
                    self._renameAndDrop(self.unionTableName, self.tableName)

            except (MySQLdb.Error, psycopg2.Error):
                #LOGGER.error("Error %d: %s", e.args[0], e.args[1])
                LOGGER.error("Fatal error encountered while ingesting '%s'", self.filePath)
                LOGGER.error("Last record ingested before failure: %d", self.lastRecordIngested)
                self.abortTime = datetime.datetime.now()
                self.didAbort = True
                self.updateStatusDict()
                raise #re-raise the exception
            #ingest completed
            self.endTime = datetime.datetime.now()
            ts = str(self.endTime - self.startTime)
            LOGGER.info("Incremental ingest of %s took %s", self.tableName, ts[:len(ts)-4])
        self.updateStatusDict()


    def connect(self, async_=0):
        """
        Establish a connection to the database, returning the connection object.
        """
        if self.dbType == "postgresql":
            conn = psycopg2.connect(
                host=self.dbHost,
                user=self.dbUser,
                password=self.dbPassword,
                database=self.dbName,
                options=("-c search_path=%s" % self.tableSchema if self.tableSchema else None),
                async_=async_
            )
        else:
            conn = MySQLdb.connect(
                charset='utf8',
                host=self.dbHost,
                user=self.dbUser,
                passwd=self.dbPassword,
                db=self.dbName)

        return conn


    def tableExists(self, tableName=None, connection=None):
        """
        Convenience method which returns True if tableName exists in the db, False if not.

        If tableName is None, uses self.tableName.

        If a connection object is specified, this method uses it and does not close it;
        if not, it creates one using connect(), uses it, and then closes it.
        """

        if self.isPostgresql:
            exStr = """SELECT COUNT(*) FROM information_schema.tables
                                WHERE table_catalog = %s
                                AND table_name = %s"""
        else:
            exStr = """SELECT COUNT(*) FROM information_schema.tables
                                WHERE table_schema = %s
                                AND table_name = %s"""

        tableName = (tableName if tableName else self.tableName)
        conn = (connection if connection else self.connect())
        cur = conn.cursor()
        cur.execute(exStr, (self.dbName, tableName))
        fet = cur.fetchone() #this will always be a 1-tuple; the items's value will be 0 or 1
        doesExist = bool(fet[0])
        cur.close()
        if not connection:
            conn.close()
        return doesExist


    def columnCount(self, tableName=None, connection=None):
        """
        Convenience method for returning the number of columns in tableName.

        If tableName is None, uses self.tableName.

        If a connection object is specified, this method uses it and does not close it;
        if not, it creates one using connect(), uses it, and then closes it.
        """
        tableName = (tableName if tableName else self.tableName)
        conn = (connection if connection else self.connect())
        cur = conn.cursor()

        if self.isPostgresql:
            exStr = """SELECT column_name, data_type, character_maximum_length FROM information_schema.columns 
                            WHERE table_name = '%s'""" % tableName
        else:
            exStr = """SHOW COLUMNS FROM %s""" % tableName

        cur.execute(exStr)
        colCount = len(cur.fetchall())
        cur.close()
        if not connection:
            conn.close()
        return colCount


    def _createTable(self, tableName):
        """
        Connect to the db and create a table named self.tableName_TMP, dropping previous one if it exists.

        Also adds primary key constraint to the new table.
        """
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""DROP TABLE IF EXISTS %s""" % tableName)
        #create the column name part of the table creation string
        colPairs = zip(self.parser.columnNames, self.parser.dataTypes)
        lst = [" ".join(aPair) for aPair in colPairs] #list comprehension
        paramStr = ",".join(lst)
        #paramString now looks like "export_date BIGINT, storefront_id INT, country_code VARCHAR(100)" etc.
        exStr = """CREATE TABLE %s (%s)""" % (tableName, paramStr)
        cur.execute(exStr) #create the table in the database
        #set the primary key
        if self.isPostgresql:
            conn.commit()
        conn.close()
        self._applyPrimaryKeyConstraints(tableName)


    def _applyPrimaryKeyConstraints(self, tableName):
        """
        Apply the primary key specified in parser to tableName.
        """
        pkLst = self.parser.primaryKey

        if pkLst:
            conn = self.connect()
            cur = conn.cursor()
            pkStr = ", ".join(pkLst)
            if self.isPostgresql:
                exStr = """ALTER TABLE %s ADD CONSTRAINT %s_pk PRIMARY KEY (%s)""" % (tableName, tableName, pkStr)
            else:
                exStr = """ALTER TABLE %s ADD CONSTRAINT PRIMARY KEY (%s)""" % (tableName, pkStr)
            cur.execute(exStr)
            if self.isPostgresql:
                conn.commit()
            conn.close()


    def _escapeRecords(self, recordList, connection=None):
        """
        Appropriately escape the contents of a list of records (as returned by the parser)
        so that there are no illegal characters (e.g. internal quotes) in the SQL query.

        This is done here rather than in the parser because it uses the literal() method of the
        connection object.
        """
        conn = (connection if connection else self.connect())
        escapedRecords = []
        cur = conn.cursor()
        keys = {}
        for aRec in recordList:
            marker = tuple([aRec[i] for i in self.parser.primaryKeyIndexes])
            if marker in keys: continue
            keys[marker] = 1
            if self.isMysql:
                escRec = [conn.literal(aField) for aField in aRec]
            else:
                escRec = [cur.mogrify("%s", (aField,)).decode("utf-8") for aField in aRec]
            escapedRecords.append(escRec)
        return escapedRecords


    def _populateTable(self, tableName, resumeNum=0, isIncremental=False, skipKeyViolators=False):
        """
        Populate tableName with data fetched by the parser, first advancing to resumePos.

        For Full imports, if skipKeyViolators is True, any insertions which would violate the primary key constraint
        will be skipped and won't log errors.
        """
        #REPLACE is a MySQL extension which inserts if the key is new, or deletes and inserts if the key is a duplicate
        commandString = ("REPLACE" if (isIncremental and self.isMysql) else "INSERT")
        ignoreString = ("IGNORE" if (skipKeyViolators and not isIncremental and self.isMysql) else "")
        colNamesStr = "(%s)" % (", ".join(self.parser.columnNames))
        conflictStr = (f' ON CONFLICT ({",".join(self.parser.primaryKey)}) DO NOTHING'
                       if self.isPostgresql and skipKeyViolators else '')
        exStrTemplate = f"""{commandString} {ignoreString} INTO {tableName} {colNamesStr} VALUES"""

        # Psycopg2 async helpers. (They don't need to be >here< here, but they're not used anywhere else)

        # Psycopg2 async wait function, as in the docs.
        def wait(conn):
            while True:
                state = conn.poll()
                if state == psycopg2.extensions.POLL_OK:
                    break
                elif state == psycopg2.extensions.POLL_WRITE:
                    select.select([], [conn.fileno()], [])
                elif state == psycopg2.extensions.POLL_READ:
                    select.select([conn.fileno()], [], [])
                else:
                    raise psycopg2.OperationalError("poll() returned %s" % state)

        # Hack. psycopg2 async mode doesn't support conn.set_client_encoding(), because that actually executes a
        # "SET client_encoding" statement on the server (and psycopg2 doesn't handle that call using the async API).
        # However, we still need the python client to know the encoding (for mogrify etc) and you get errors about
        # _py_enc being None if it's not set at all. This hack doesn't actually set it at the libpq level; so the
        # locale (LC_*) should be set to en_US.UTF-8 too. Modified from psycopg2 cursor.py
        def psycopg2_async_set_client_encoding(conn, encoding):
            from psycopg2cffi._impl import encodings as _enc
            encoding = _enc.normalize(encoding)
            if conn.encoding == encoding:
                return

            pyenc = _enc.encodings[encoding]
            conn._encoding = encoding
            conn._py_enc = pyenc

        self.parser.seekToRecord(resumeNum) #advance to resumeNum
        if self.isPostgresql:
            conn_idx = 0
            conns = [self.connect(async_=1) for _ in range(8)]
            for conn in conns:
                psycopg2_async_set_client_encoding(conn, 'UTF8')
            curs = [conn.cursor() for conn in conns]
        else:
            conn = self.connect()

        while (True):
            #By default, we concatenate 200 inserts into a single INSERT statement.
            #a large batch size per insert improves performance, until you start hitting max_packet_size issues.
            #If you increase MySQL server's max_packet_size, you may get increased performance by increasing maxNum
            records = self.parser.nextRecords(maxNum=10000)
            if (not records):
                break

            escapedRecords = self._escapeRecords(records) #This will sanitize the records
            stringList = [(", ".join(aRecord)) for aRecord in escapedRecords]

            if self.isMysql:
                cur = conn.cursor()

            exStr = f"{exStrTemplate} ({'), ('.join(stringList)}){conflictStr}"

            try:
                if self.isPostgresql:
                    while True:
                        state = conns[conn_idx].poll()
                        if state == psycopg2.extensions.POLL_OK:
                            break
                        conn_idx += 1
                        conn_idx = conn_idx % 8
                    curs[conn_idx].execute(exStr)
                else:
                    cur.execute(exStr)
            except (MySQLdb.Warning, psycopg2.Warning) as e:
                LOGGER.warning(str(e))
            except (MySQLdb.IntegrityError, psycopg2.IntegrityError) as e:
                # This is likely a primary key constraint violation; should only be hit if skipKeyViolators is False
                LOGGER.error(str(e))
            except (MySQLdb.Error, psycopg2.Error):
                LOGGER.error("error executing %s" % exStr)
                raise  # re-raise the exception

            self.lastRecordIngested = self.parser.latestRecordNum
            recCheck = self._checkProgress()
            if recCheck:
                LOGGER.info(
                    "...at record %i...",
                    recCheck
                )

        if self.isPostgresql:
            for conn in conns:
                try:
                    wait(conn)
                except:
                    # don't fail a whole import if one of the final batches contained a key constraint failure.
                    # this matches the behaviour in the try/except LOGGER.warning block above.
                    pass
                conn.close()
            conn = self.connect()

        LOGGER.info("Ingested %i records", self.lastRecordIngested)
        self._createCustomIndexes(self.fileName.split(".")[0], tableName)

        LOGGER.info("Analyzing table")
        cur = conn.cursor()
        cur.execute(f'ANALYZE {tableName}')
        conn.commit()

        conn.close()

    def _checkProgress(self, recordGap=5000, timeGap=datetime.timedelta(0, 120, 0)):
        """
        Checks whether recordGap or more records have been ingested since the last check;
        if so, checks whether timeGap seconds have elapsed since the last check.

        If both checks pass, returns self.lastRecordIngested; otherwise returns None.
        """
        if self.lastRecordIngested - self.lastRecordCheck >= recordGap:
            t = datetime.datetime.now()
            if t - self.lastTimeCheck >= timeGap:
                self.lastTimeCheck = t
                self.lastRecordCheck = self.lastRecordIngested
                return self.lastRecordCheck
        return None


    def _dropTable(self, tableName):
        """A convenience method that just connects, drops tableName if it exists, and disconnects"""
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""DROP TABLE IF EXISTS %s""" % tableName)
        if self.isPostgresql:
            conn.commit()
        conn.close()


    def _createCustomIndexes(self, fileName, tableName):
        # fileName here is pretty much the "final" table name in DB.
        # tableName is the name of the temporary table being used for loading.
        if self.isPostgresql:
            custom_indexes_sql = {
                'artist_collection': f'CREATE INDEX ON {tableName} (collection_id)',
                'collection': f'CREATE INDEX ON {tableName} (lower(name) text_pattern_ops)',
                'artist': f'CREATE INDEX ON {tableName} (lower(name) text_pattern_ops)',
            }
            sql = custom_indexes_sql.get(fileName)
            if sql:
                conn = self.connect()
                cur = conn.cursor()
                LOGGER.info(f"Creating custom {fileName} index")
                cur.execute(sql)
                conn.commit()
                conn.close()


    def _renameAndDrop(self, sourceTable, targetTable):
        """
        Temporarily rename targetTable, then rename sourceTable to targetTable.
        If this succeeds, drop the renamed targetTable; otherwise revert it and drop sourceTable.
        """
        conn = self.connect()
        cur = conn.cursor()
        revert = False
        #first, rename the existing "real" table, so we can restore it if something goes wrong
        targetOld = targetTable + "_old"
        cur.execute("""DROP TABLE IF EXISTS %s""" % targetOld)
        if self.isMysql:
            exStr = """ALTER %s %s RENAME %s"""
        else:
            exStr = """ALTER %s %s RENAME TO %s"""

        if (self.tableExists(targetTable, connection=conn)):
            cur.execute(exStr % ("TABLE", targetTable, targetOld))
            if self.isPostgresql:
                cur.execute(exStr % ("INDEX", targetTable+'_pk', targetOld+'_pk'))
                conn.commit()
        #now rename the new table to replace the old table
        try:
            cur.execute(exStr % ("TABLE", sourceTable, targetTable))
            if self.isPostgresql:
                cur.execute(exStr % ("INDEX", sourceTable+'_pk', targetTable+'_pk'))
                conn.commit()
        except MySQLdb.Error as e:
            LOGGER.error("Error %d: %s", e.args[0], e.args[1])
            revert = True
        except psycopg2.Error as e:
            LOGGER.error("Error %s", e)
            revert = True

        if revert:
            LOGGER.error("Could not rename tmp table; reverting to original table (if it exists).")
            if (self.tableExists(targetOld, connection=conn)):
                cur.execute(exStr % ("TABLE", targetOld, targetTable))
                if self.isPostgresql:
                    cur.execute(exStr % ("INDEX", targetOld+'_pk', targetTable+'_pk'))
                    conn.commit()
        #Drop sourceTable so it's not hanging around
        #drop the old table
        cur.execute("""DROP TABLE IF EXISTS %s""" % targetOld)
        if self.isPostgresql:
            conn.commit()
        conn.close()


    def _createUnionTable(self):
        """
        After incremental ingest data has been written to self.incTableName, union the pruned
        original table and the new table into a tmp table
        """
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""DROP TABLE IF EXISTS %s""" % self.unionTableName)

        if self.isPostgresql:
            exStr = """CREATE TABLE %s AS %s""" % (self.unionTableName, self._incrementalUnionString())
        else:
            exStr = """CREATE TABLE %s %s""" % (self.unionTableName, self._incrementalUnionString())

        cur.execute(exStr)
        if self.isPostgresql:
            conn.commit()
        conn.close()


    def _incrementalWhereClause(self):
        """
        Creates and returns the appropriate WHERE clause string used when pruning the target table
        during an incremental ingest
        """
        pCols = self.parser.primaryKey
        substrings = ["%s.%s=%s.%s" % (self.tableName, aCol, self.incTableName, aCol) for aCol in pCols]
        joinedString = " AND ".join(substrings)
        whereClause = "WHERE %s.export_date <= %s.export_date AND %s" % (self.tableName, self.incTableName, joinedString)
        return whereClause


    def _incrementalSelectString(self):
        """
        Creates and returns the appropriate SELECT statement used when pruning the target table
        during an incremental ingest
        """
        whereClause = self._incrementalWhereClause()
        selectString = ("SELECT * FROM %s WHERE 0 = (SELECT COUNT(*) FROM %s %s)" %
            (self.tableName, self.incTableName, whereClause))
        return selectString


    def _incrementalUnionString(self):
        """
        Creates and returns the appropriate UNION string used when merging the pruned table
        with the temporary incrmental table.

        The ingest and pruning process should preclude any dupes, so we can use ALL, which should be faster.
        """
        selectString = self._incrementalSelectString()
        if self.isPostgresql:
            unionString = "SELECT * FROM %s UNION ALL %s" % (self.incTableName, selectString)
        else:
            unionString = "IGNORE SELECT * FROM %s UNION ALL %s" % (self.incTableName, selectString)
        return unionString
