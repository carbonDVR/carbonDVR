#!/usr/bin/env python3.4

from datetime import timezone
import logging
import os
import os.path
import sys
import time

import pytz

import bifGen
import cleanup
#import fetchXTVD
import fileLocations
#import parseXTVD
import recorder
from sqliteDatabase import SqliteDatabase
import transcoder
import webServer


class ConfigHolder:
    pass


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

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
    carbonDVRConfig.webserverPort = getMandatoryEnvVar('CARBONDVR_WEBSERVER_PORT')
    carbonDVRConfig.listingsFetchTime = time.strptime(getMandatoryEnvVar('CARBONDVR_LISTINGS_FETCH_TIME'), '%H:%M:%S')
    carbonDVRConfig.fileLocations = fileLocations.FileLocations(getMandatoryEnvVar('CARBONDVR_FILE_LOCATIONS'))
    logger.info('Listing fetch time: %02d:%02d:00', carbonDVRConfig.listingsFetchTime.tm_hour, carbonDVRConfig.listingsFetchTime.tm_min)

    fetchXTVDConfig = ConfigHolder()
    fetchXTVDConfig.schedulesDirectUsername = getMandatoryEnvVar('SCHEDULES_DIRECT_USERNAME')
    fetchXTVDConfig.schedulesDirectPassword = getMandatoryEnvVar('SCHEDULES_DIRECT_PASSWORD')
    fetchXTVDConfig.listingsFile = getMandatoryEnvVar('CARBONDVR_LISTINGS_FILE')

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
    restConfig.restServerURL = getMandatoryEnvVar('RESTSERVER_RESTSERVER_URL')

    dbConnection = SqliteDatabase("/opt/carbonDVR/lib/carbonDVR.sqlite")

    scheduler = BackgroundScheduler(timezone=pytz.utc)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)            # turn down the logging from apscheduler
    logging.getLogger('apscheduler.scheduler').setLevel(logging.ERROR)    # turn down the logging from apscheduler

    recorderDBInterface = dbConnection
    channels = recorderDBInterface.getChannels()
    tuners = recorderDBInterface.getTuners()
    hdhomerun = recorder.HDHomeRunInterface(channels, tuners, recorderConfig.hdhomerunBinary)
    recorder = recorder.Recorder(scheduler, hdhomerun, recorderDBInterface, recorderConfig.videoFilespec, recorderConfig.logFilespec)

    transcoderDB = dbConnection
    transcoder = transcoder.Transcoder(transcoderDB, transcoderConfig.lowCommand, transcoderConfig.mediumCommand, transcoderConfig.highCommand,
        transcoderConfig.outputFilespec, transcoderConfig.logFilespec)
    scheduler.add_job(transcoder.transcodeRecordings, trigger=IntervalTrigger(seconds=60))

    bifGenDB = dbConnection
    bifGen = bifGen.BifGen(bifGenDB, bifGenConfig.imageCommand, bifGenConfig.imageDir, bifGenConfig.bifFilespec, bifGenConfig.frameInterval)
    scheduler.add_job(bifGen.bifRecordings, trigger=IntervalTrigger(seconds=60))

    cleanupDB = dbConnection
    cleanup = cleanup.Cleanup(cleanupDB)
    scheduler.add_job(cleanup.cleanup, trigger=IntervalTrigger(minutes=60))

# temporarily disable XTVD while we're migrating to carbon.trinaria.com
#    def fetchListings():
#        fetchXTVD.fetchXTVDtoFile(fetchXTVDConfig.schedulesDirectUsername, fetchXTVDConfig.schedulesDirectPassword, fetchXTVDConfig.listingsFile)
#        dbInterface = parseXTVD.carbonDVRDatabase(dbConnection, carbonDVRConfig.schema)
#        parseXTVD.parseXTVD(fetchXTVDConfig.listingsFile, dbInterface)
#    fetchTrigger = CronTrigger(hour = carbonDVRConfig.listingsFetchTime.tm_hour, minute = carbonDVRConfig.listingsFetchTime.tm_min)
#    scheduler.add_job(fetchListings, trigger=fetchTrigger, misfire_grace_time=3600)


# temporarily disable scheduler, while we're migrating to carbon.trinaria.com
#    scheduler.start();


    def scheduleRecordingsCallback():
        recorder.scheduleRecordings()

    logging.getLogger('werkzeug').setLevel(logging.WARNING)            # turn down the logging from werkzeug
    restServerDB = dbConnection
    webServer.webServerApp.restServer = webServer.RestServer(restServerDB, carbonDVRConfig.fileLocations, restConfig.restServerURL)
    uiServerDB = dbConnection
    webServer.webServerApp.uiServer = webServer.UIServer(uiServerDB, uiConfig.uiServerURL, scheduleRecordingsCallback)

    # There's something about the web vivaldi browser that cause flask to block, so that can't process requests from other clients.
    # If the vivaldi browser makes a second request, any pending requests from other clients are serviced, then the vivaldi request
    # is serviced, and then we're back to being blocked so that only the vivaldi browser can make requests.
    # Likely culprit is some kind of keep-alive mechanism.
    # As a (dubious and risky) workaround, enable multithreading in flask, so that other threads are available to service the non-vivaldi clients.
#    webServer.webServerApp.run(host='0.0.0.0',port=int(carbonDVRConfig.webserverPort), threaded=True, debug=True)
#    webServer.webServerApp.run(host='0.0.0.0',port=int(carbonDVRConfig.webserverPort), threaded=True)
    webServer.webServerApp.run(host='0.0.0.0',port=int(carbonDVRConfig.webserverPort), threaded=False)

