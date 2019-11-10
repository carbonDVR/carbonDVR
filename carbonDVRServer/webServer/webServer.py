#!/usr/bin/env python3.4

import flask
import logging
import os
import psycopg2
import sys

webServerApp = flask.Flask(__name__)


#
# UI Server Endpoints
#

@webServerApp.route('/')
def getIndex():
    return flask.current_app.uiServer.getIndex()

@webServerApp.route('/recordingsByDate')
def getRecordingsByDate():
    return flask.current_app.uiServer.getRecordingsByDate()

@webServerApp.route('/recentRecordings')
def getRecentRecordings():
    return flask.current_app.uiServer.getRecentRecordings()

@webServerApp.route('/upcomingRecordings')
def getUpcomingRecordings():
    return flask.current_app.uiServer.getUpcomingRecordings()

@webServerApp.route('/showList')
def getShowList():
    return flask.current_app.uiServer.getShowList()

@webServerApp.route('/subscribe/<showID>')
def subscribe(showID):
    flask.current_app.uiServer.subscribe(showID)
    return flask.redirect(flask.url_for('getShowList'))

@webServerApp.route('/unsubscribe/<showID>')
def unsubscribe(showID):
    flask.current_app.uiServer.unsubscribe(showID)
    return flask.redirect(flask.url_for('getShowList'))

@webServerApp.route('/databaseInconsistencies')
def getDatabaseInconsistencies():
    return flask.current_app.uiServer.getDatabaseInconsistencies()

@webServerApp.route('/scheduleTestRecording')
def scheduleTestRecording():
    flask.current_app.uiServer.scheduleTestRecording()
    return flask.redirect(flask.url_for('getUpcomingRecordings'))

@webServerApp.route('/recordingsByShow')
def getRecordingsByShow():
    return flask.current_app.uiServer.getRecordingsByShow()

@webServerApp.route('/transcodingFailures')
def getTranscodingFailures():
    return flask.current_app.uiServer.getTranscodingFailures()

@webServerApp.route('/pendingTranscodingJobs')
def getPendingTranscodingJobs():
    return flask.current_app.uiServer.getPendingTranscodingJobs()

@webServerApp.route('/retryTranscode/<recordingID>')
def retryTranscode(recordingID):
    flask.current_app.uiServer.retryTranscode(recordingID)
    return flask.redirect(flask.url_for('getTranscodingFailures'))

@webServerApp.route('/deleteRecordingFromRecordingsByShow/<recordingID>')
def deleteRecordingFromRecordingsByShow(recordingID):
    flask.current_app.restServer.deleteRecording(recordingID)
    return flask.redirect(flask.url_for('getRecordingsByShow'))


#
# REST Server Endpoints
#

@webServerApp.route('/shows')
def getAllShows():
    return flask.current_app.restServer.getAllShows()

@webServerApp.route('/shows/new')
def getShowsWithNewEpisodes():
    return flask.current_app.restServer.getShowsWithNewEpisodes()

@webServerApp.route('/shows/<showID>/episodes/new')
def getShowEpisodesNew(showID):
    return flask.current_app.restServer.getShowEpisodesNew(showID)

@webServerApp.route('/shows/<showID>/episodes/rerun')
def getShowEpisodesRerun(showID):
    return flask.current_app.restServer.getShowEpisodesRerun(showID)

@webServerApp.route('/shows/<showID>/episodes/archive')
def getShowEpisodesArchive(showID):
    return flask.current_app.restServer.getShowEpisodesArchive(showID)

@webServerApp.route('/recordings/<recordingID>')
def getRecording(recordingID):
    return flask.current_app.restServer.getRecording(recordingID)

@webServerApp.route('/recordings/<recordingID>', methods=['DELETE'])
def deleteRecording(recordingID):
    return flask.current_app.restServer.deleteRecording(recordingID)

@webServerApp.route('/recordings/<recordingID>/playbackPosition')
def getPlaybackPosition(recordingID):
    return flask.current_app.restServer.getPlaybackPosition(recordingID)

@webServerApp.route('/recordings/<recordingID>/playbackPosition/<playbackPosition>', methods=['PUT'])
def setPlaybackPosition(recordingID, playbackPosition):
    return flask.current_app.restServer.setPlaybackPosition(recordingID, playbackPosition)

@webServerApp.route('/recordings/<recordingID>/archiveState')
def getArchiveState(recordingID):
    return flask.current_app.restServer.getArchiveState(recordingID)

@webServerApp.route('/recordings/<recordingID>/archiveState/1', methods=['PUT'])
def archiveRecording(recordingID):
    return flask.current_app.restServer.archiveRecording(recordingID)

@webServerApp.route('/alarms')
def getAlarms():
    alarmList = flask.current_app.restServer.getAlarms()
    return flask.jsonify({"alarmList":alarmList})

