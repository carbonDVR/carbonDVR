#!/usr/bin/env python3.4

import os, os.path
import logging
import io
import psycopg2
import datetime
import subprocess


class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


def dbGetUnreferencedRawVideoRecords(dbConnection):
    records = []
    cursor = dbConnection.cursor()
    query = str('SELECT recording_id, filename '
                'FROM file_raw_video '
                'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                'ORDER BY recording_id;')
    cursor.execute(query)
    for row in cursor:
        records.append(Bunch(recordingID=row[0], filename=row[1]))
    cursor.close()
    dbConnection.commit()
    return records


def dbDeleteRawVideoRecord(dbConnection, recordingID):
    cursor = dbConnection.cursor()
    cursor.execute('DELETE FROM file_raw_video WHERE recording_id = %s', (recordingID, ))
    cursor.close()
    dbConnection.commit()


def purgeUnreferencedRawVideoRecords(dbConnection):
    logger = logging.getLogger(__name__)
    for record in dbGetUnreferencedRawVideoRecords(dbConnection):
       logger.info('Deleting file: {}'.format(record.filename))
       try:
           os.unlink(record.filename)
       except FileNotFoundError:
           logger.info('File not found: {}'.format(record.filename))
       dbDeleteRawVideoRecord(dbConnection, record.recordingID)


def dbGetUnreferencedTranscodedVideoRecords(dbConnection):
    records = []
    cursor = dbConnection.cursor()
    query = str('SELECT recording_id, filename '
                'FROM file_transcoded_video '
                'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                'ORDER BY recording_id;')
    cursor.execute(query)
    for row in cursor:
        records.append(Bunch(recordingID=row[0], filename=row[1]))
    cursor.close()
    dbConnection.commit()
    return records


def dbDeleteTranscodedVideoRecord(dbConnection, recordingID):
    cursor = dbConnection.cursor()
    cursor.execute('DELETE FROM file_transcoded_video WHERE recording_id = %s', (recordingID, ))
    cursor.close()
    dbConnection.commit()


def purgeUnreferencedTranscodedVideoRecords(dbConnection):
    logger = logging.getLogger(__name__)
    for record in dbGetUnreferencedTranscodedVideoRecords(dbConnection):
       logger.info('Deleting file: {}'.format(record.filename))
       try:
           os.unlink(record.filename)
       except FileNotFoundError:
           logger.info('File not found: {}'.format(record.filename))
       dbDeleteTranscodedVideoRecord(dbConnection, record.recordingID)


def dbGetUnreferencedBifRecords(dbConnection):
    records = []
    cursor = dbConnection.cursor()
    query = str('SELECT recording_id, filename '
                'FROM file_bif '
                'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                'ORDER BY recording_id;')
    cursor.execute(query)
    for row in cursor:
        records.append(Bunch(recordingID=row[0], filename=row[1]))
    cursor.close()
    dbConnection.commit()
    return records


def dbDeleteBifRecord(dbConnection, recordingID):
    cursor = dbConnection.cursor()
    cursor.execute('DELETE FROM file_bif WHERE recording_id = %s', (recordingID, ))
    cursor.close()
    dbConnection.commit()


def purgeUnreferencedBifRecords(dbConnection):
    logger = logging.getLogger(__name__)
    for record in dbGetUnreferencedBifRecords(dbConnection):
       logger.info('Deleting file: {}'.format(record.filename))
       try:
           os.unlink(record.filename)
       except FileNotFoundError:
           logger.info('File not found: {}'.format(record.filename))
       dbDeleteBifRecord(dbConnection, record.recordingID)


def dbGetUnneededRawVideoRecords(dbConnection):
    records = []
    cursor = dbConnection.cursor()
    query = str('SELECT file_raw_video.recording_id, file_raw_video.filename '
                'FROM file_raw_video '
                'INNER JOIN file_transcoded_video USING (recording_id) '
                'WHERE file_transcoded_video.status = 0 '
                'ORDER BY file_raw_video.recording_id;')
    cursor.execute(query)
    for row in cursor:
        records.append(Bunch(recordingID=row[0], filename=row[1]))
    cursor.close()
    dbConnection.commit()
    return records


def purgeUnneededRawVideoRecords(dbConnection):
    logger = logging.getLogger(__name__)
    for record in dbGetUnneededRawVideoRecords(dbConnection):
       logger.info('Deleting file: {}'.format(record.filename))
       try:
           os.unlink(record.filename)
       except FileNotFoundError:
           logger.info('File not found: {}'.format(record.filename))
       dbDeleteRawVideoRecord(dbConnection, record.recordingID)



def main():
    FORMAT = "%(asctime)-15s: %(name)s:  %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    logger = logging.getLogger(__name__)

    import configparser
    config = configparser.ConfigParser()
    config.read('/opt/trintv/etc/trinTV.conf')
    configSection = config['cleanup']
    dbConnectString = configSection['dbConnectString']
    dbConnection = psycopg2.connect(dbConnectString)

    logger.info('Purging unreferenced raw video records')
    purgeUnreferencedRawVideoRecords(dbConnection)

    logger.info('Purging unreferenced transcoded video records')
    purgeUnreferencedTranscodedVideoRecords(dbConnection)

    logger.info('Purging unreferenced BIF records')
    purgeUnreferencedBifRecords(dbConnection)

    logger.info('Purging raw video files that have been transcoded')
    purgeUnneededRawVideoRecords(dbConnection)


if __name__ == '__main__':
    main()
