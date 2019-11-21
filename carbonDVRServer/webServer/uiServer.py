#!/usr/bin/env python3.4

import logging
import os

from bunch import Bunch
from flask import render_template
import tzlocal


class ShowData:
    pass


class UIServer:
    def __init__(self, db, uiServerURL, scheduleRecordingsCallback):
        self.db = db
        self.uiServerURL = uiServerURL
        self.scheduleRecordingsCallback = scheduleRecordingsCallback

    def makeURL(self, endpoint):
        return self.uiServerURL + endpoint

    def getIndex(self):
        return render_template('index.html')

    def getRecordingsByDate(self):
        recordings = self.db.getAllRecordings()
        for recording in recordings:
            recording.dateRecorded = recording.dateRecorded.astimezone(tzlocal.get_localzone())
        return render_template('recordingsByDate.html', recordings=recordings)

    def getRecentRecordings(self):
        recordings = self.db.getRecentRecordings()
        for recording in recordings:
            recording.dateRecorded = recording.dateRecorded.astimezone(tzlocal.get_localzone())
        return render_template('recentRecordings.html', recordings=recordings)

    def getUpcomingRecordings(self):
        schedules = self.db.getUpcomingRecordings()
        for schedule in schedules:
            schedule.startTime = schedule.startTime.astimezone(tzlocal.get_localzone())
        return render_template('upcomingRecordings.html', schedules=schedules)

    def getShowList(self):
        shows = self.db.getShowList()
        return render_template('showList.html', subscribedShows=shows.subscribed, unsubscribedShows=shows.unsubscribed)

    def subscribe(self, showID):
        self.db.subscribe(showID)

    def unsubscribe(self, showID):
        self.db.unsubscribe(showID)

    def getDatabaseInconsistencies(self):
        inconsistencies = self.db.getInconsistencies()
        return render_template('databaseInconsistencies.html', recordingsWithoutFileRecords=inconsistencies.recordingsWithoutFileRecords,
            fileRecordsWithoutRecordings=inconsistencies.fileRecordsWithoutRecordings, rawVideoFilesThatCanBeDeleted=inconsistencies.rawVideoFilesThatCanBeDeleted)

    def scheduleTestRecording(self):
        self.db.scheduleTestRecording()
        self.scheduleRecordingsCallback()

    def getRecordingsByShow(self):
        allRecordings = self.db.getAllRecordings()
        for recording in allRecordings:
            recording.dateRecorded = recording.dateRecorded.astimezone(tzlocal.get_localzone())
        showList = []
        recordingsByShow = {}
        for showName in set([x.show for x in allRecordings]):
          recordings = [x for x in allRecordings if x.show == showName]
          showData = ShowData()
          showData.name=showName
          showData.numRecordings=len(recordings)
          showList.append(showData)
          recordingsByShow[showData] = recordings
        showList = sorted(showList, key=lambda x: x.name)
        return render_template('recordingsByShow.html', showList=showList, recordingsByShow=recordingsByShow)

    def getTranscodingFailures(self):
        transcodingFailures = self.db.getTranscodingFailures()
        return render_template('transcodingFailures.html', recordings=transcodingFailures)

    def getPendingTranscodingJobs(self):
        pendingTranscodingJobs = self.db.getPendingTranscodingJobs()
        return render_template('pendingTranscodingJobs.html', recordings=pendingTranscodingJobs)

    def retryTranscode(self, recordingID):
        self.db.deleteFailedTranscode(recordingID)

