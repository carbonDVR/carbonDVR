#!/usr/bin/env python3.4

import sys, os, os.path
import logging
import psycopg2
import pytz

import recorder
import uiServer


from apscheduler.schedulers.background import BlockingScheduler

def getMandatoryEnvVar(varName):
    logger = logging.getLogger(__name__)
    value = os.environ.get(varName)
    if value is None:
        logger.error('%s environment variable is not set', varName)
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
    logFilespec = getMandatoryEnvVar('VIDEO_LOG_FILESPEC')
    commandPipeFilespec = getMandatoryEnvVar('RECORDER_COMMUNICATION_PIPE')

    dbConnection = psycopg2.connect(dbConnectString)

    schema = os.environ.get('DB_SCHEMA')
    if schema is not None:
        logger.info('DB_SCHEMA=%s', schema)
        with dbConnection.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (schema, ))
        dbConnection.commit()

    scheduler = BlockingScheduler(timezone=pytz.utc)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)    # turn down the logging from apscheduler

    recorderDBInterface = recorder.CarbonDVRDatabase(dbConnection)
    channels = recorderDBInterface.getChannels()
    tuners = recorderDBInterface.getTuners()
    hdhomerun = recorder.HDHomeRunInterface(channels, tuners, hdhomerunBinary)
    recorder = recorder.Recorder(scheduler, hdhomerun, recorderDBInterface, videoFilespec, logFilespec, commandPipeFilespec)

    uiServer.uiServerApp.dbConnection = dbConnection
    uiServer.uiServerApp.pipeToRecorder = commandPipeFilespec
    scheduler.add_job(uiServer.uiServerApp.run(host='0.0.0.0',port=8085,debug=True), misfire_grace_time=86400)
#    scheduler.add_job(uiServer.uiServerApp.run(host='0.0.0.0',port=8085), misfire_grace_time=86400)


    scheduler.start();

