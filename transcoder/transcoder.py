#!/usr/bin/env python3.4

import os, os.path
import logging
import time
import subprocess
import io
import psycopg2
import datetime



def getMegabitsPerSecond(filename, duration):
    # returns bitrate of file, in Mb/s
    if not os.path.isfile(filename):
        return 0
    if duration.total_seconds() == 0:
        return 0
    filesize = os.path.getsize(filename)
    return int((filesize/duration.total_seconds())/125000)



class Transcoder:

    def __init__(self, dbConnection, transcoderLow, transcoderMedium, transcoderHigh, outputFilespec, logFilespec):
        self.logger = logging.getLogger(__name__)
        self.dbConnection = dbConnection
        self.ffmpegCommand_low = transcoderLow
        self.ffmpegCommand_medium = transcoderMedium
        self.ffmpegCommand_high = transcoderHigh
        self.transcodedVideoFilespec = outputFilespec
        self.logFilespec = logFilespec
        self.isTranscoding = False
        self.logger.debug("Template ffmpeg command (low): {}".format(self.ffmpegCommand_low))
        self.logger.debug("Template ffmpeg command (medium): {}".format(self.ffmpegCommand_medium))
        self.logger.debug("Template ffmpeg command (high): {}".format(self.ffmpegCommand_high))
        self.logger.debug("Transcoded video filespec: {}".format(self.transcodedVideoFilespec))
        self.logger.debug("Log filespec: {}".format(self.logFilespec))

    def dbSelectRecordingsToTranscode(self):
        recordings = []
        with self.dbConnection.cursor() as cursor:
            cursor.execute("SELECT recording_id, filename FROM file_raw_video WHERE recording_id NOT IN (SELECT recording_id FROM file_transcoded_video);")
            for row in cursor:
                recordings.append({'recordingID':row[0], 'filename':row[1]})
        return recordings

    def dbGetDuration(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("SELECT duration FROM recording WHERE recording_id = %s;", (recordingID,))
            row = cursor.fetchone()
            if row is None:
                return datetime.timedelta(seconds=0)
            return row[0]

    def dbInsertTranscodedFileLocation(self, recordingID, filename, state):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO file_transcoded_video(recording_id, filename, state) VALUES (%s, %s, %s)", (recordingID, filename, state))

    def transcode(self, recordingID, sourceFile, destFile, logFile, duration):
        self.logger.info("Transcoding {} to {}".format(sourceFile, destFile))
        sourceBitrate = getMegabitsPerSecond(sourceFile, duration)
        self.logger.info('Source file bitrate is {}Mb/s (avg)'.format(sourceBitrate))
        if sourceBitrate == 0:
             # error reading source rate, default to medium quality
            cmd = self.ffmpegCommand_medium.format(recordingID=recordingID)
        elif sourceBitrate < 3:
            cmd = self.ffmpegCommand_low.format(recordingID=recordingID)
        elif sourceBitrate < 8:
            cmd = self.ffmpegCommand_medium.format(recordingID=recordingID)
        else:
            cmd = self.ffmpegCommand_high.format(recordingID=recordingID)
        self.logger.info("ffmpeg command: {}".format(cmd))
        logFileHandle = io.open(logFile, "w+")
        result = subprocess.call(cmd.split(), stdout=logFileHandle, stderr=subprocess.STDOUT)
        logFileHandle.close()
        self.logger.info("Exit code: {}".format(result))
        return result == 0

    def transcodeRecordings(self):
        if self.isTranscoding:
            return
        self.isTranscoding = True
        recordings = self.dbSelectRecordingsToTranscode()
        for recording in recordings[:1]:
            recordingID = recording['recordingID']
            srcFile = recording['filename']
            destFile = self.transcodedVideoFilespec.format(recordingID=recordingID)
            logFile = self.logFilespec.format(recordingID=recordingID)
            duration = self.dbGetDuration(recordingID)
            if self.transcode(recordingID, srcFile, destFile, logFile, duration):
                self.logger.info("Transcode successful")
                self.dbInsertTranscodedFileLocation(recordingID, destFile, 0)
            else:
                self.logger.info("Transcode failed")
                self.dbInsertTranscodedFileLocation(recordingID, destFile, 1)
        self.isTranscoding = False

