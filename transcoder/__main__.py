#!/usr/bin/env python3.4

import sys, os, os.path
import logging
import psycopg2
import pytz

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from transcoder import Transcoder



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
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    logger = logging.getLogger(__name__)

    dbConnectString = getMandatoryEnvVar('DB_CONNECT_STRING')
    transcoderLow = getMandatoryEnvVar('TRANSCODER_COMMAND_LOW')
    transcoderMedium = getMandatoryEnvVar('TRANSCODER_COMMAND_MEDIUM')
    transcoderHigh = getMandatoryEnvVar('TRANSCODER_COMMAND_HIGH')
    outputFilespec = getMandatoryEnvVar('TRANSCODER_VIDEO_FILESPEC')
    logFilespec = getMandatoryEnvVar('TRANSCODER_LOG_FILESPEC')

    dbConnection = psycopg2.connect(dbConnectString)

    schema = os.environ.get('DB_SCHEMA')
    if schema is not None:
        logger.info('DB_SCHEMA=%s', schema)
        with dbConnection.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (schema, ))
        dbConnection.commit()

    transcoder = Transcoder(dbConnection, transcoderLow, transcoderMedium, transcoderHigh, outputFilespec, logFilespec)

    scheduler = BlockingScheduler(timezone=pytz.utc)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)    # turn down the logging from apscheduler
    scheduler.add_job(transcoder.transcodeRecordings, trigger=IntervalTrigger(seconds=60))
    scheduler.start();

