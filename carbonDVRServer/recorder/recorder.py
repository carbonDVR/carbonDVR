import pytz
import logging
import threading

from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
from .hdhomerun import UnrecognizedChannelException, NoTunersAvailableException, BadRecordingException



class Recorder:
    def __init__(self, scheduler, hdhomerunInterface, dbInterface, videoFilespec, logFilespec):
        self.logger = logging.getLogger(__name__)
        self.schedulingLock = threading.Lock()
        self.scheduler = scheduler
        self.hdhomerunInterface = hdhomerunInterface
        self.dbInterface = dbInterface
        self.videoFilespec = videoFilespec
        self.logFilespec = logFilespec
        self.scheduleRecordings()
        self.scheduler.add_job(self.scheduleRecordings, trigger=CronTrigger(hour='0,6,12,18', minute='40'), misfire_grace_time=600)

    def removeAllRecordingJobs(self):
        self.logger.debug('Removing recording jobs')
        for job in self.scheduler.get_jobs():
            if job.func == self.record:
                self.logger.debug('Removing job: {}'.format(job))
                self.scheduler.remove_job(job.id)

    def scheduleRecordings(self):
        with self.schedulingLock:
            self.logger.info("Scheduling recordings")
            self.removeAllRecordingJobs()
            pendingRecordings = self.dbInterface.getPendingRecordings()
            pendingRecordings.sort(key=lambda pendingRecording: pendingRecording.startTime) # not really necessary, just makes log files easier to follow
            for pendingRecording in pendingRecordings:
                self.logger.info("Scheduling recording on channel {}-{} at {}".
                    format(pendingRecording.channelMajor, pendingRecording.channelMinor, pendingRecording.startTime.astimezone(pytz.timezone('US/Central'))))
                self.scheduler.add_job(self.record, args = [pendingRecording], trigger = 'date', run_date = pendingRecording.startTime, misfire_grace_time=60)

    def record(self, schedule):
        self.logger.info("Recording channel {}-{}".format(schedule.channelMajor, schedule.channelMinor))
        recordingID = self.dbInterface.getUniqueID()
        destinationFile = self.videoFilespec.format(recordingID=recordingID)
        logFile = self.logFilespec.format(recordingID=recordingID)
        stopTime = schedule.startTime + schedule.duration
        self.dbInterface.insertRecording(recordingID, schedule.showID, schedule.episodeID, schedule.duration, schedule.rerunCode)
        try:
            self.hdhomerunInterface.record(schedule.channelMajor, schedule.channelMinor, stopTime, destinationFile, logFile)
            self.logger.info("Successfully recorded")
            self.dbInterface.insertRawVideoLocation(recordingID, destinationFile);
        except (UnrecognizedChannelException, NoTunersAvailableException, BadRecordingException):
            self.logger.error("Recording failed")


