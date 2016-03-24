#!/usr/bin/env python3.4

from flask import Flask, g, abort, render_template, redirect, request, url_for, current_app
import logging
import os
import psycopg2
import sys

webServerApp = Flask(__name__)


#
# UI Server Endpoints
#

@webServerApp.route('/')
def getIndex():
    return current_app.uiServer.getIndex()

@webServerApp.route('/recordings')
def getAllRecordings():
    return current_app.uiServer.getAllRecordings()

@webServerApp.route('/recentRecordings')
def getRecentRecordings():
    return current_app.uiServer.getRecentRecordings()

@webServerApp.route('/upcomingRecordings')
def getUpcomingRecordings():
    return current_app.uiServer.getUpcomingRecordings()

@webServerApp.route('/showList')
def getShowList():
    return current_app.uiServer.getShowList()

@webServerApp.route('/subscribe/<showID>')
def subscribe(showID):
    current_app.uiServer.subscribe(showID)
    return redirect(url_for('getShowList'))

@webServerApp.route('/unsubscribe/<showID>')
def unsubscribe(showID):
    current_app.uiServer.unsubscribe(showID)
    return redirect(url_for('getShowList'))

@webServerApp.route('/databaseInconsistencies')
def getDatabaseInconsistencies():
    return current_app.uiServer.getDatabaseInconsistencies()

@webServerApp.route('/scheduleTestRecording')
def scheduleTestRecording():
    current_app.uiServer.scheduleTestRecording()
    return redirect(url_for('getUpcomingRecordings'))




#
# REST Server Endpoints
#

@webServerApp.route('/shows')
def getAllShows():
    return current_app.restServer.getAllShows()

@webServerApp.route('/shows/new')
def getShowsWithNewEpisodes():
    return current_app.restServer.getShowsWithNewEpisodes()

@webServerApp.route('/shows/<showID>/episodes/new')
def getShowEpisodesNew(showID):
    return current_app.restServer.getShowEpisodesNew(showID)

@webServerApp.route('/shows/<showID>/episodes/rerun')
def getShowEpisodesRerun(showID):
    return current_app.restServer.getShowEpisodesRerun(showID)

@webServerApp.route('/shows/<showID>/episodes/archive')
def getShowEpisodesArchive(showID):
    return current_app.restServer.getShowEpisodesArchive(showID)

@webServerApp.route('/recordings/<recordingID>')
def getRecording(recordingID):
    return current_app.restServer.getRecording(recodingID)

@webServerApp.route('/recordings/<recordingID>', methods=['DELETE'])
def deleteRecording(recordingID):
    return current_app.restServer.deleteRecorder(recordingID)

@webServerApp.route('/recordings/<recordingID>/playbackPosition')
def getPlaybackPosition(recordingID):
    return current_app.restServer.getPlaybackPosition(recordingID)

@webServerApp.route('/recordings/<recordingID>/playbackPosition/<playbackPosition>', methods=['PUT'])
def setPlaybackPosition(recordingID, playbackPosition):
    return current_app.restServer.setPlaybackPosition(recordingID, playbackPosition)

@webServerApp.route('/recordings/<recordingID>/archiveState')
def getArchiveState(recordingID):
    return current_app.restServer.getArchiveState(recordingID)

@webServerApp.route('/recordings/<recordingID>/archiveState/1', methods=['PUT'])
def archiveRecording(recordingID):
    return current_app.restServer.archiveRecording(recordingID)

