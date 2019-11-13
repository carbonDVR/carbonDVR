#!/usr/bin/env python3.4

import psycopg2
import logging
import os
import subprocess
import tempfile
import threading


def getFiles(path):
    files = []
    for entry in os.listdir(path):
        spec = os.path.join(path, entry)
        if os.path.isfile(spec):
            files.append(spec)
    return files


def getFilesByExt(path, targetExt):
    files = []
    for entry in os.listdir(path):
        spec = os.path.join(path, entry)
        ext = os.path.splitext(entry)[1]
        if os.path.isfile(spec) and ext.casefold() == targetExt.casefold():
            files.append(spec)
    return files


# function to generate BIF files from image files
# downloaded from: https://bitbucket.org/bcl/homevideo/src/tip/server/makebif.py
import struct
import array
def makeBIF( filename, directory, interval ):
    """
    Build a .bif file for the Roku Player Tricks Mode

    @param filename name of .bif file to create
    @param directory Directory of image files 00000001.jpg
    @param interval Time, in milliseconds, between the images
    """
    magic = [0x89,0x42,0x49,0x46,0x0d,0x0a,0x1a,0x0a]
    version = 0

    files = os.listdir("%s" % (directory))
    images = []
    for image in files:
        if image[-4:] == '.jpg':
            images.append(image)
    images.sort()

    f = open(filename, "wb")
    array.array('B', magic).tofile(f)
    f.write(struct.pack("<1I", version))
    f.write(struct.pack("<1I", len(images)))
    f.write(struct.pack("<1I", int(interval)))
    array.array('B', [0x00 for x in range(20,64)]).tofile(f)

    bifTableSize = 8 + (8 * len(images))
    imageIndex = 64 + bifTableSize
    timestamp = 0

    # Get the length of each image
    for image in images:
        statinfo = os.stat("%s/%s" % (directory, image))
        f.write(struct.pack("<1I", timestamp))
        f.write(struct.pack("<1I", imageIndex))

        timestamp += 1
        imageIndex += statinfo.st_size

    f.write(struct.pack("<1I", 0xffffffff))
    f.write(struct.pack("<1I", imageIndex))

    # Now copy the images
    for image in images:
        data = open("%s/%s" % (directory, image), "rb").read()
        f.write(data)

    f.close()


class BifGenDB_Postgres:
    def __init__(self, dbConnection):
        self.dbConnection = dbConnection

    def getRecordingsToBif(self):
        recordings = []
        with self.dbConnection.cursor() as cursor:
            cursor.execute("SELECT recording_id, filename FROM file_transcoded_video WHERE state = %s AND recording_id NOT IN (SELECT recording_id FROM file_bif);", (0, ))
            for row in cursor:
                recordings.append({'recordingID':row[0], 'filename':row[1]})
        self.dbConnection.commit()
        return recordings

    def insertBifFileLocation(self, recordingID, locationID, filename):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO file_bif(recording_id, location_id, filename) VALUES (%s, %s, %s)", (recordingID, locationID, filename))
        self.dbConnection.commit()



class BifGen:

    def __init__(self, db, imageCommand, imageDir, bifFilespec, frameInterval):
        self.logger = logging.getLogger(__name__)
        self.workingLock = threading.Lock()
        self.db = db
        self.ffmpegCommand = imageCommand
        self.imageDir = imageDir
        self.bifFilespec = bifFilespec
        self.frameInterval = frameInterval
        self.logger.debug("Template ffmepg command: {}".format(self.ffmpegCommand))
        self.logger.debug("Image directory: {}".format(self.imageDir))
        self.logger.debug("BIF filespec: {}".format(self.bifFilespec))
        self.logger.debug("Frame interval: {}ms".format(self.frameInterval))

    def clearImageDirectory(self):
        self.logger.debug("Clearing image directory {}".format(self.imageDir))
        for file in getFilesByExt(self.imageDir, '.jpg'):
            os.unlink(file)

    def imageFile(self, fileNumber):
        return os.path.join(self.imageDir, '{:0>8}.jpg'.format(fileNumber))
#
# Notes on BIF process
#
# The "-itsoffset -1" in the ffmpeg command is to generate the image files 1 second before the actual video timestamp.  This helps to make the result of
# ffwd/rewind seem more natural.
#     The stream cannot start playing until an I-frame, so if your images are exactly lined up with the timestamp in the video, it's almost guaranteed
#     that the video will start playing a few moments *after* the image in the ffwd/rewind.
#
# After ffmpeg has generated the thumbnails, we have to renumber them.  When ffmpeg generates them, the files are numbered starting from 00000001, but
# biftool wants the files to be numbered from 00000000
#
#

    def bifRecording(self, recording):
        recordingID = recording['recordingID']
        self.logger.info("Biffing recording {}".format(recordingID))
        # generate thumbnails
        self.clearImageDirectory()
        framesPerSecond = 1000 / self.frameInterval
        cmd = self.ffmpegCommand.format(videoFile=recording['filename'], framesPerSecond=framesPerSecond, imageDir=self.imageDir)
        self.logger.info("Running ffmpeg ({})".format(cmd))
        outfile = tempfile.TemporaryFile("w+")
        subprocess.call(cmd.split(), stdout=outfile, stderr=subprocess.STDOUT)
        # renumber thumbnails
        self.logger.debug("Renumbering thumbnails")
        i = 0
        while os.path.isfile(self.imageFile(i+1)):
            os.rename(self.imageFile(i+1), self.imageFile(i))
            i = i + 1
        # generate BIF file
        locationID = 1
        bifFile = self.bifFilespec.format(recordingID=recordingID)
        self.logger.info("Generating BIF file {}".format(bifFile))
        makeBIF(bifFile, self.imageDir, self.frameInterval)
        # mark recording as "biffed"
        self.db.insertBifFileLocation(recordingID, locationID, bifFile)
        # cleanup
        self.clearImageDirectory()
        self.logger.info("BIF generation complete")

    def bifRecordings(self):
        with self.workingLock:
            recordings = self.db.getRecordingsToBif()
            for recording in recordings[:1]:
                self.bifRecording(recording)

