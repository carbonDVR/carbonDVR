#!/usr/bin/env python3.4

import os, os.path
import logging
import io
import psycopg2
import datetime
import subprocess
import threading


class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)

class CleanupDB_Postgres:
    def __init__(self, dbConnection):
        self.dbConnection = dbConnection

    def getUnreferencedRawVideoRecords(self):
        records = []
        query = str('SELECT recording_id, filename '
                    'FROM file_raw_video '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id;')
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                records.append(Bunch(recordingID=row[0], filename=row[1]))
        self.dbConnection.commit()
        return records

    def deleteRawVideoRecord(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM file_raw_video WHERE recording_id = %s', (recordingID, ))
        self.dbConnection.commit()

    def getUnreferencedTranscodedVideoRecords(self):
        records = []
        query = str('SELECT recording_id, filename '
                    'FROM file_transcoded_video '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id;')
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                records.append(Bunch(recordingID=row[0], filename=row[1]))
        self.dbConnection.commit()
        return records

    def deleteTranscodedVideoRecord(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM file_transcoded_video WHERE recording_id = %s', (recordingID, ))
        self.dbConnection.commit()

    def getUnreferencedBifRecords(self):
        records = []
        query = str('SELECT recording_id, filename '
                    'FROM file_bif '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id;')
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                records.append(Bunch(recordingID=row[0], filename=row[1]))
        self.dbConnection.commit()
        return records

    def deleteBifRecord(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM file_bif WHERE recording_id = %s', (recordingID, ))
        self.dbConnection.commit()

    def getUnneededRawVideoRecords(self):
        records = []
        query = str('SELECT file_raw_video.recording_id, file_raw_video.filename '
                    'FROM file_raw_video '
                    'INNER JOIN file_transcoded_video USING (recording_id) '
                    'WHERE file_transcoded_video.state = 0 '
                    'ORDER BY file_raw_video.recording_id;')
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                records.append(Bunch(recordingID=row[0], filename=row[1]))
        self.dbConnection.commit()
        return records


class Cleanup:
    def __init__(self, db):
        self.cleaningLock = threading.Lock()
        self.db = db

    def purgeUnreferencedRawVideoRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.dbGetUnreferencedRawVideoRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.db.deleteRawVideoRecord(record.recordingID)

    def purgeUnreferencedTranscodedVideoRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.dbGetUnreferencedTranscodedVideoRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.db.deleteTranscodedVideoRecord(record.recordingID)


    def purgeUnreferencedBifRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.db.getUnreferencedBifRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.db.deleteBifRecord(record.recordingID)


    def purgeUnneededRawVideoRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.db.getUnneededRawVideoRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.db.deleteRawVideoRecord(record.recordingID)

    def cleanup(self):
        logger = logging.getLogger(__name__)
        with self.cleaningLock:
            logger.info('Purging unneeded files')
            logger.debug('Purging unreferenced raw video records')
            self.purgeUnreferencedRawVideoRecords()
            logger.debug('Purging unreferenced transcoded video records')
            self.purgeUnreferencedTranscodedVideoRecords()
            logger.debug('Purging unreferenced BIF records')
            self.purgeUnreferencedBifRecords()
            logger.debug('Purging raw video files that have been transcoded')
            self.purgeUnneededRawVideoRecords()

