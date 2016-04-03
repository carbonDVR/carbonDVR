#!/usr/bin/env python

import datetime
import logging
import os, os.path
import io
import subprocess
import signal
import threading
import time
import pytz

from bunch import Bunch


# we're not really checking much here, but it's better than nothing
# at least it will detect 0-byte files
def isaValidRecording(filename):
    if not os.path.isfile(filename):
        return False
    filesize = os.path.getsize(filename)
    if filesize is None:
        return False
    if filesize > 10000000 :  # make sure file is at least 10MB
        return True
    else:
        return False



class ChannelMap:
    def __init__(self, channelList):     
        self.channelDict = {}
        for channel in channelList:
            self.addChannel(channel.channelMajor, channel.channelMinor, channel.channelActual, channel.program)  

    def addChannel(self, channelMajor, channelMinor, channelActual, program):
        key = (channelMajor, channelMinor)
        self.channelDict[key] = Bunch(channelMajor=channelMajor, channelMinor=channelMinor, channelActual=channelActual, program=program)

    def getChannelInfo(self, channelMajor, channelMinor):
        key = (channelMajor, channelMinor)
        if key in self.channelDict:
            return self.channelDict[key]
        else:
            return None


class TunerList:
    def __init__(self, tunerList):
        self.lock = threading.Lock()
        self.tuners = []
        self.lockedTuners = []
        for tuner in tunerList:
            self.addTuner(tuner.deviceID, tuner.ipAddress, tuner.tunerID)

    def addTuner(self, deviceID, ipAddress, tunerID):
        self.tuners.append(Bunch(deviceID=deviceID, ipAddress=ipAddress, tunerID=tunerID))

    def lockTuner(self):
        with self.lock:
            if not self.tuners:
                return None
            tuner = self.tuners[0]
            self.tuners.remove(tuner)
            self.lockedTuners.append(tuner)
            return tuner

    def releaseTuner(self, tuner):
        with self.lock:
            if tuner in self.lockedTuners:
                self.lockedTuners.remove(tuner)
                self.tuners.append(tuner)


class UnrecognizedChannelException(Exception):
    pass


class NoTunersAvailableException(Exception):
    pass


class BadRecordingException(Exception):
    pass


class HDHomeRunInterface:
    def __init__(self, channels, tuners, hdhomerunBinary):
        self.channelMap = ChannelMap(channels)
        self.tunerList = TunerList(tuners)
        self.hdhomerunBinary = hdhomerunBinary
        self.logger = logging.getLogger(__name__)

    def record(self, channelMajor, channelMinor, endTime, destFile, logFile):
        self.logger.info("Recording: Channel={}-{}, EndTime={}, Filename={}".format(channelMajor, channelMinor, endTime, destFile))
        # get channel and tuner info
        channelInfo = self.channelMap.getChannelInfo(channelMajor, channelMinor)
        if channelInfo == None:
            self.logger.error("Unrecognized Channel: {}-{}".format(channelMajor, channelMinor))
            raise UnrecognizedChannelException
        tuner = self.tunerList.lockTuner()
        if tuner == None:
            self.logger.error("No tuners available")
            raise NoTunersAvailableException
        self.logger.info("Selected tuner {}:{}".format(tuner.deviceID, tuner.tunerID))
        # setup logfile
        self.logger.info("Logging to {}".format(logFile))
        logFileHandle = io.open(logFile, "w+")
        # set tuner to channel
        cmd = [self.hdhomerunBinary, tuner.ipAddress, "set", '/tuner{}/channel'.format(tuner.tunerID), '{}'.format(channelInfo.channelActual)]
        self.logger.info("Tuning channel: {}".format(cmd))
        subprocess.Popen(cmd, stdout=logFileHandle, stderr=subprocess.STDOUT).wait()
        # set tuner to program
        cmd = [self.hdhomerunBinary, tuner.ipAddress, "set", '/tuner{}/program'.format(tuner.tunerID), '{}'.format(channelInfo.program)]
        self.logger.info("Selecting program: {}".format(cmd))
        subprocess.Popen(cmd, stdout=logFileHandle, stderr=subprocess.STDOUT).wait()
        # check tuner status
        cmd = [self.hdhomerunBinary, tuner.ipAddress, "get", '/tuner{}/status'.format(tuner.tunerID)]
        self.logger.info("Checking tuner status: {}".format(cmd))
        subprocess.Popen(cmd, stdout=logFileHandle, stderr=subprocess.STDOUT).wait()
        # start recording
        cmd = [self.hdhomerunBinary, tuner.ipAddress, "save", '/tuner{}'.format(tuner.tunerID), destFile]
        self.logger.info("Recording: {}".format(cmd))
        processHandle = subprocess.Popen(cmd, stdout=logFileHandle, stderr=subprocess.STDOUT)
        # sleep until time to stop recording
        currentTime = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)  # for reasons which beggar the imagination, 'utcnow' returns a datatime w/o a timezone
        duration = endTime - currentTime
        self.logger.info("Recording for {} seconds".format(duration.total_seconds()))
        time.sleep(duration.total_seconds())
        self.logger.info("Sending SIGTERM to process {}".format(processHandle.pid))
        os.kill(processHandle.pid, signal.SIGTERM)
        processHandle.wait()
        logFileHandle.close()
        # release tuner
        self.tunerList.releaseTuner(tuner)
        self.logger.info("Finished recording: Channel={}-{}, Duration={}s, Filename={}".format(channelMajor, channelMinor, duration, destFile))
        # did we actually get a recording?
        if not isaValidRecording(destFile):
            self.logger.info("Recording failed on tuner {}:{}".format(tuner.deviceID, tuner.tunerID))
            raise BadRecordingException
        self.logger.info("Recording succeeded on tuner {}:{}".format(tuner.deviceID, tuner.tunerID))

