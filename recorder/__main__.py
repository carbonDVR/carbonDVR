#!/usr/bin/env python3.4

import sys, os, os.path
import logging
import psycopg2

from carbonDVRDatabase import CarbonDVRDatabase
from hdhomerun import HDHomeRun
from recorder import Recorder

def getMandatoryEnvVar(varName):
    logger = logging.getLogger(__name__)
    value = os.environ.get(varName)
    if value is None:
        logger.error('%s environment variable is not set', envVar)
        sys.exit(1)
    logger.info('%s=%s', varName, value)
    return value


if __name__ == '__main__':
    FORMAT = "%(asctime)-15s: %(name)s:  %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    dbConnectString = getMandatoryEnvVar('DB_CONNECT_STRING')
    hdhomerunBinary = getMandatoryEnvVar('HDHOMERUN_BINARY')
    videoFilespec = getMandatoryEnvVar('VIDEO_FILESPEC')
    logFilespec = getMandatoryEnvVar('LOG_FILESPEC')

    dbConnection = psycopg2.connect(dbConnectString)

    schema = os.environ.get('DB_SCHEMA')
    if schema is not None:
        logger.info('DB_SCHEMA=%s', schema)
        with dbConnection.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (schema, ))

    dbInterface = carbonDVRDatabase(dbConnection, schema)

    channels = dbInterface.getChannels()
    tuners = dbInterface.getTuners()
    hdhomerun = HDHomeRun(channels, tuner, hdhomerunBinary)

    recorder = Recorder(hdhomerun, dbInterface, videoFilespec, logFilespec)
    recorder.run()

