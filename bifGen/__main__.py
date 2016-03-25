#!/usr/bin/env python3.4

import sys, os, os.path
import logging
import psycopg2
import pytz

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from bifGen import BifGen


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
    bifCommand = getMandatoryEnvVar('BIFGEN_BIF_COMMAND')
    imageCommand = getMandatoryEnvVar('BIFGEN_IMAGE_COMMAND')
    workingDir = getMandatoryEnvVar('BIFGEN_WORKING_DIR')
    imageDir = getMandatoryEnvVar('BIFGEN_IMAGE_DIR')
    bifFilespec = getMandatoryEnvVar('BIFGEN_BIF_FILESPEC')
    frameInterval = int(getMandatoryEnvVar('BIFGEN_FRAME_INTERVAL'))

    dbConnection = psycopg2.connect(dbConnectString)

    schema = os.environ.get('DB_SCHEMA')
    if schema is not None:
        logger.info('DB_SCHEMA=%s', schema)
        with dbConnection.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (schema, ))
        dbConnection.commit()

    bifGen = BifGen(dbConnection, imageCommand, bifCommand, workingDir, imageDir, bifFilespec, frameInterval)

    scheduler = BlockingScheduler(timezone=pytz.utc)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)    # turn down the logging from apscheduler
    scheduler.add_job(bifGen.bifRecordings, trigger=IntervalTrigger(seconds=60))
    scheduler.start();

