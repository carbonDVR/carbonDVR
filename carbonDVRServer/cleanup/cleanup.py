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

class Cleanup:
    def __init__(self, dbConnection):
        self.cleaningLock = threading.Lock()
        self.dbConnection = dbConnection

    def dbGetUnreferencedRawVideoRecords(self):
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


    def dbDeleteRawVideoRecord(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM file_raw_video WHERE recording_id = %s', (recordingID, ))
        self.dbConnection.commit()


    def purgeUnreferencedRawVideoRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.dbGetUnreferencedRawVideoRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.dbDeleteRawVideoRecord(record.recordingID)


    def dbGetUnreferencedTranscodedVideoRecords(self):
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


    def dbDeleteTranscodedVideoRecord(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM file_transcoded_video WHERE recording_id = %s', (recordingID, ))
        self.dbConnection.commit()


    def purgeUnreferencedTranscodedVideoRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.dbGetUnreferencedTranscodedVideoRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.dbDeleteTranscodedVideoRecord(record.recordingID)


    def dbGetUnreferencedBifRecords(self):
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


    def dbDeleteBifRecord(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM file_bif WHERE recording_id = %s', (recordingID, ))
        self.dbConnection.commit()


    def purgeUnreferencedBifRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.dbGetUnreferencedBifRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.dbDeleteBifRecord(record.recordingID)


    def dbGetUnneededRawVideoRecords(self):
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


    def purgeUnneededRawVideoRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.dbGetUnneededRawVideoRecords():
           logger.info('Deleting file: {}'.format(record.filename))
           try:
               os.unlink(record.filename)
           except FileNotFoundError:
               logger.info('File not found: {}'.format(record.filename))
           self.dbDeleteRawVideoRecord(record.recordingID)

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

