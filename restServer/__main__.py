#!/usr/bin/env python3.4

import sys, os, os.path
import logging
import psycopg2
import pytz

import restServer
from apscheduler.schedulers.background import BlockingScheduler

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
    genericSDPosterURL = getMandatoryEnvVar('RESTSERVER_GENERIC_SD_POSTER_URL')
    genericHDPosterURL = getMandatoryEnvVar('RESTSERVER_GENERIC_HD_POSTER_URL')
    streamURL = getMandatoryEnvVar('RESTSERVER_STREAM_URL')
    bifURL = getMandatoryEnvVar('RESTSERVER_BIF_URL')

    dbConnection = psycopg2.connect(dbConnectString)

    schema = os.environ.get('DB_SCHEMA')
    if schema is not None:
        logger.info('DB_SCHEMA=%s', schema)
        with dbConnection.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (schema, ))
        dbConnection.commit()

    scheduler = BlockingScheduler(timezone=pytz.utc)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)    # turn down the logging from apscheduler

    restServer.app.dbConnection = dbConnection
    restServer.globalConfig.genericSDPosterURL = genericSDPosterURL
    restServer.globalConfig.genericHDPosterURL = genericHDPosterURL
    restServer.globalConfig.streamURL = streamURL
    restServer.globalConfig.bifURL = bifURL
#    scheduler.add_job(restServer.app.run(host='0.0.0.0',port=8086,debug=True), misfire_grace_time=86400)
#    scheduler.add_job(restServer.app.run(host='0.0.0.0',port=8086), misfire_grace_time=86400)
    scheduler.add_job(restServer.app.run(host='0.0.0.0',port=8081,debug=True), misfire_grace_time=86400)
    scheduler.start();

