#!/usr/bin/env python3.4

import argparse
from datetime import timezone
import logging
import time
import yaml

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from bunch import Bunch
import pytz

from bifGen import BifGen
from cleanup import Cleanup
from fetchXTVD import fetchXTVDtoFile
import fileLocations
from parseXTVD import parseXTVD
from recorder import HDHomeRunInterface, Recorder
from sqliteDatabase import SqliteDatabase
from transcoder import Transcoder
import webServer


if __name__ == '__main__':
    FORMAT = "%(asctime)-15s: %(name)s:  %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Run carbonDVR server.')
    parser.add_argument('-c', '--configFile', dest='configFile', default='/opt/carbonDVR/etc/carbonDVR.cfg')
    args = parser.parse_args()

    with open(args.configFile) as yamlFile:
      config = yaml.load(yamlFile)

    generalConfig = Bunch(
        listenPort = config['general']['listenPort'],
        dbFile = config['general']['dbFile'])
    logger.info('Listen port: %d', generalConfig.listenPort)
    logger.info('Database file: %s', generalConfig.dbFile)

    xtvdConfig = Bunch(
        schedulesDirectUsername = config['xtvd']['schedulesDirectUsername'],
        schedulesDirectPassword = config['xtvd']['schedulesDirectPassword'],
        listingsFetchTime = time.strptime(config['xtvd']['listingsFetchTime'], '%H:%M:%S'),
        listingsFile = config['xtvd']['listingsFile'])
    logger.info('Listing fetch time: %02d:%02d:00', xtvdConfig.listingsFetchTime.tm_hour, xtvdConfig.listingsFetchTime.tm_min)
    logger.info('Listing file: %s', xtvdConfig.listingsFile)

    recorderConfig = Bunch(
        hdhomerunBinary = config['recorder']['hdhomerunBinary'],
        videoFilespec = config['recorder']['videoFilespec'],
        logFilespec = config['recorder']['logFilespec'])
    logger.info('hdhomerunBinary: %s', recorderConfig.hdhomerunBinary)
    logger.info('recorder videoFilespec: %s', recorderConfig.videoFilespec)
    logger.info('recorder logFilespec: %s', recorderConfig.logFilespec)

    transcoderConfig = Bunch(
        lowCommand = config['transcoder']['lowCommand'].rstrip(),
        mediumCommand = config['transcoder']['mediumCommand'].rstrip(),
        highCommand = config['transcoder']['highCommand'].rstrip(),
        outputFilespec = config['transcoder']['outputFilespec'],
        logFilespec = config['transcoder']['logFilespec'])
    logger.info('transcoder low resolution transcode command: %s', transcoderConfig.lowCommand)
    logger.info('transcoder medium resolution transcode command: %s', transcoderConfig.mediumCommand)
    logger.info('transcoder high resolution transcode command: %s', transcoderConfig.highCommand)
    logger.info('transcoder outputFilespec: %s', transcoderConfig.outputFilespec)
    logger.info('transcoder logFilespec: %s', transcoderConfig.logFilespec)

    bifGenConfig = Bunch(
        imageCommand = config['bifGen']['imageCommand'],
        imageDir = config['bifGen']['imageDir'],
        bifFilespec = config['bifGen']['bifFilespec'],
        frameInterval = int(config['bifGen']['frameInterval']))
    logger.info('bifGen imageCommand: %s', bifGenConfig.imageCommand)
    logger.info('bifGen imageDir: %s', bifGenConfig.imageDir)
    logger.info('bifGen bifFilespec: %s', bifGenConfig.bifFilespec)
    logger.info('bifGen frameInterval: %s', bifGenConfig.frameInterval)

    uiConfig = Bunch(
        uiServerURL = config['uiServer']['uiServerURL'])
    logger.info('uiServer uiServerURL: %s', uiConfig.uiServerURL)

    restConfig = Bunch(
        restServerURL = config['restServer']['restServerURL'],
        fileLocations = config['general']['fileLocations'])
    logger.info('restServer restServerURL: %s', restConfig.restServerURL)
    logger.info('File locations file: %s', restConfig.fileLocations)


    scheduler = BackgroundScheduler(timezone=pytz.utc)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)            # turn down the logging from apscheduler
    logging.getLogger('apscheduler.scheduler').setLevel(logging.ERROR)    # turn down the logging from apscheduler

    dbConnection = SqliteDatabase(generalConfig.dbFile)

    channels = dbConnection.getChannels()
    tuners = dbConnection.getTuners()
    hdhomerun = HDHomeRunInterface(channels, tuners, recorderConfig.hdhomerunBinary)
    recorder = Recorder(scheduler, hdhomerun, dbConnection, recorderConfig.videoFilespec, recorderConfig.logFilespec)

    transcoder = Transcoder(dbConnection, transcoderConfig.lowCommand, transcoderConfig.mediumCommand, transcoderConfig.highCommand,
        transcoderConfig.outputFilespec, transcoderConfig.logFilespec)
    scheduler.add_job(transcoder.transcodeRecordings, trigger=IntervalTrigger(seconds=60))

    bifGen = BifGen(dbConnection, bifGenConfig.imageCommand, bifGenConfig.imageDir, bifGenConfig.bifFilespec, bifGenConfig.frameInterval)
    scheduler.add_job(bifGen.bifRecordings, trigger=IntervalTrigger(seconds=60))

    cleanup = Cleanup(dbConnection)
    scheduler.add_job(cleanup.cleanup, trigger=IntervalTrigger(minutes=60))

# temporarily disable XTVD while we're migrating to carbon.trinaria.com
#    def fetchListings():
#        fetchXTVDtoFile(xtvdConfig.schedulesDirectUsername, xtvdConfig.schedulesDirectPassword, xtvdConfig.listingsFile)
#        parseXTVD(xtvdConfig.listingsFile, dbConnection)
#    fetchTrigger = CronTrigger(hour = xtvdConfig.listingsFetchTime.tm_hour, minute = xtvdConfig.listingsFetchTime.tm_min)
#    scheduler.add_job(fetchListings, trigger=fetchTrigger, misfire_grace_time=3600)

    scheduler.start();

    def scheduleRecordingsCallback():
        recorder.scheduleRecordings()

    logging.getLogger('werkzeug').setLevel(logging.WARNING)            # turn down the logging from werkzeug
    fileLocations = fileLocations.FileLocations(jsonFile=restConfig.fileLocations)
    webServer.webServerApp.restServer = webServer.RestServer(dbConnection, fileLocations, restConfig.restServerURL)
    webServer.webServerApp.uiServer = webServer.UIServer(dbConnection, uiConfig.uiServerURL, scheduleRecordingsCallback)

    # There's something about the web vivaldi browser that cause flask to block, so that can't process requests from other clients.
    # If the vivaldi browser makes a second request, any pending requests from other clients are serviced, then the vivaldi request
    # is serviced, and then we're back to being blocked so that only the vivaldi browser can make requests.
    # Likely culprit is some kind of keep-alive mechanism.
    # As a (dubious and risky) workaround, enable multithreading in flask, so that other threads are available to service the non-vivaldi clients.
#    webServer.webServerApp.run(host='0.0.0.0',port=generalConfig.listenPort, threaded=True, debug=True)
    webServer.webServerApp.run(host='0.0.0.0',port=generalConfig.listenPort, threaded=True)

