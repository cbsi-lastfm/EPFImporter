from setuptools import setup, find_packages

from version import version

setup(
    name='EPFImporter',
    version=version,
    packages=find_packages(),
    long_description='A Python CLI tool for importing Apple Enterprise Partner Feed data, '
                     'available to EPF partners, into a relational database.',
    install_requires=[
        'configparser==5.0.2',
        'PyMySQL==1.0.2',
        'psycopg2cffi==2.9.0',
    ],
)
