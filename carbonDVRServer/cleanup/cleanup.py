#!/usr/bin/env python3.4

import os
import logging
import subprocess
import threading


class Cleanup:
    def __init__(self, db):
        self.cleaningLock = threading.Lock()
        self.db = db

    def purgeUnreferencedRawVideoRecords(self):
        logger = logging.getLogger(__name__)
        for record in self.db.getUnreferencedRawVideoRecords():
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

