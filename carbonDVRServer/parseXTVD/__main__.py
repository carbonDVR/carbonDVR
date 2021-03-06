#!/usr/bin/env python3.4

import argparse
import logging
import os
import parseXTVD
import psycopg2
import sqlite3
import sys


def importIntoPostgresql(xtvdFile):
    dbConnectString = os.environ.get('DB_CONNECT_STRING')
    if dbConnectString is None:
        print('DB_CONNECT_STRING environment variable is not set')
        sys.exit(1)
    logger.info('DB_CONNECT_STRING=%s', dbConnectString)
    dbConnection = psycopg2.connect(dbConnectString)

    schema = os.environ.get('DB_SCHEMA')
    if schema is not None:
        logger.info('DB_SCHEMA=%s', schema)
        with dbConnection.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (schema, ))

    dbInterface = parseXTVD.carbonDVRDatabase(dbConnection, schema)

    parseXTVD.parseXTVD(xtvdFile, dbInterface)



def importIntoSqlite(xtvdFile, sqliteDatabase):
    dbConnection = sqlite3.connect(sqliteDatabase)
    dbInterface = parseXTVD.sqliteDatabase(dbConnection)
    parseXTVD.parseXTVD(xtvdFile, dbInterface)



if __name__ == '__main__':
    FORMAT = "%(asctime)-15s: %(name)s:  %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Parse listings in XTVD format.')
    parser.add_argument('-f', '--file', default='data.xml')
    args = parser.parse_args()

#    importIntoPostgresql()
    importIntoSqlite(args.file, '/opt/carbonDVR/var/carbonDVR.sqlite')

