#!/usr/bin/env python3.4

import sys, os, os.path
import logging
import psycopg2
import pytz

import recorder
import transcoder
import bifGen
import cleanup
import webServer

class ConfigHolder:
    pass


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

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

    carbonDVRConfig = ConfigHolder()
    carbonDVRConfig.dbConnectString = getMandatoryEnvVar('CARBONDVR_DB_CONNECT_STRING')
    carbonDVRConfig.schema = getMandatoryEnvVar('CARBONDVR_DB_SCHEMA')
    carbonDVRConfig.webserverPort = getMandatoryEnvVar('CARBONDVR_WEBSERVER_PORT')

    recorderConfig = ConfigHolder()
    recorderConfig.hdhomerunBinary = getMandatoryEnvVar('RECORDER_HDHOMERUN_BINARY')
    recorderConfig.videoFilespec = getMandatoryEnvVar('RECORDER_VIDEO_FILESPEC')
    recorderConfig.logFilespec = getMandatoryEnvVar('RECORDER_VIDEO_LOG_FILESPEC')

    transcoderConfig = ConfigHolder()
    transcoderConfig.lowCommand = getMandatoryEnvVar('TRANSCODER_COMMAND_LOW')
    transcoderConfig.mediumCommand = getMandatoryEnvVar('TRANSCODER_COMMAND_MEDIUM')
    transcoderConfig.highCommand = getMandatoryEnvVar('TRANSCODER_COMMAND_HIGH')
    transcoderConfig.outputFilespec = getMandatoryEnvVar('TRANSCODER_VIDEO_FILESPEC')
    transcoderConfig.logFilespec = getMandatoryEnvVar('TRANSCODER_LOG_FILESPEC')

    bifGenConfig = ConfigHolder()
    bifGenConfig.imageCommand = getMandatoryEnvVar('BIFGEN_IMAGE_COMMAND')
    bifGenConfig.imageDir = getMandatoryEnvVar('BIFGEN_IMAGE_DIR')
    bifGenConfig.bifFilespec = getMandatoryEnvVar('BIFGEN_BIF_FILESPEC')
    bifGenConfig.frameInterval = int(getMandatoryEnvVar('BIFGEN_FRAME_INTERVAL'))

    uiConfig = ConfigHolder()
    uiConfig.uiServerURL = getMandatoryEnvVar('UISERVER_UISERVER_URL')

    restConfig = ConfigHolder()
    restConfig.genericSDPosterURL = getMandatoryEnvVar('RESTSERVER_GENERIC_SD_POSTER_URL')
    restConfig.genericHDPosterURL = getMandatoryEnvVar('RESTSERVER_GENERIC_HD_POSTER_URL')
    restConfig.restServerURL = getMandatoryEnvVar('RESTSERVER_RESTSERVER_URL')
    restConfig.streamURL = getMandatoryEnvVar('RESTSERVER_STREAM_URL')
    restConfig.bifURL = getMandatoryEnvVar('RESTSERVER_BIF_URL')

    dbConnection = psycopg2.connect(carbonDVRConfig.dbConnectString)
    if carbonDVRConfig.schema is not None:
        with dbConnection.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (carbonDVRConfig.schema, ))
        dbConnection.commit()

    scheduler = BackgroundScheduler(timezone=pytz.utc)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)            # turn down the logging from apscheduler
    logging.getLogger('apscheduler.scheduler').setLevel(logging.ERROR)    # turn down the logging from apscheduler

    recorderDBInterface = recorder.CarbonDVRDatabase(dbConnection)
    channels = recorderDBInterface.getChannels()
    tuners = recorderDBInterface.getTuners()
    hdhomerun = recorder.HDHomeRunInterface(channels, tuners, recorderConfig.hdhomerunBinary)
    recorder = recorder.Recorder(scheduler, hdhomerun, recorderDBInterface, recorderConfig.videoFilespec, recorderConfig.logFilespec)

    transcoder = transcoder.Transcoder(dbConnection, transcoderConfig.lowCommand, transcoderConfig.mediumCommand, transcoderConfig.highCommand,
        transcoderConfig.outputFilespec, transcoderConfig.logFilespec)
    scheduler.add_job(transcoder.transcodeRecordings, trigger=IntervalTrigger(seconds=60))

    bifGen = bifGen.BifGen(dbConnection, bifGenConfig.imageCommand, bifGenConfig.imageDir, bifGenConfig.bifFilespec, bifGenConfig.frameInterval)
    scheduler.add_job(bifGen.bifRecordings, trigger=IntervalTrigger(seconds=60))

    cleanup = cleanup.Cleanup(dbConnection)
    scheduler.add_job(cleanup.cleanup, trigger=IntervalTrigger(minutes=60))

    scheduler.start();


    def scheduleRecordingsCallback():
        recorder.scheduleRecordings()

    webServer.webServerApp.restServer = webServer.RestServer(dbConnection, restConfig.genericSDPosterURL, restConfig.genericHDPosterURL, restConfig.restServerURL, restConfig.streamURL, restConfig.bifURL)
    webServer.webServerApp.uiServer = webServer.UIServer(dbConnection, uiConfig.uiServerURL, scheduleRecordingsCallback)
#    webServer.webServerApp.run(host='0.0.0.0',port=int(carbonDVRConfig.webserverPort), debug=True)
    webServer.webServerApp.run(host='0.0.0.0',port=int(carbonDVRConfig.webserverPort))

