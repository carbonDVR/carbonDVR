#!/usr/bin/env python3.4

import datetime
import json
import os
from xml.sax import saxutils


class Bunch():
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


# We really would prefer to use JSON, instead of XML, but versions of the Roku SDK prior to 4.8 do not support JSON,
# and version 4.8 is not available for series 1 Roku units.
# So, we're stuck with XML

# To reduce transmission size, the TrinTV Roku app uses XML attributes, rather than child nodes, to hold values
def dictionaryToRokuXml(tag, d):

    # tag + attributes
    xml = '<' + saxutils.escape(tag) + ' '
    for key, value in d.items():
        if type(value) is not list and type(value) is not dict:
            xml += saxutils.escape(key) + '=' + saxutils.quoteattr(str(value)) + ' '
    xml += '>'

    # contents
    for key, value in d.items():
        if type(value) is dict:
            xml += dictionaryToRokuXml(key, value)

    # closing tag
    xml += '</' + saxutils.escape(tag) + '>'

    return xml


def listToRokuXml(listTag, itemTag, l):
    xml = '<' + saxutils.escape(listTag) + '>\n'
    for item in l:
        if type(item) is dict:
            xml += dictionaryToRokuXml(itemTag, item)
    xml += '</' + saxutils.escape(listTag) + '>\n'
    return xml


def formatTime(t):
    # the options in strftime are too limited to produce the output I'm looking for
    dayOfWeek = t.strftime("%a")
    monthName = t.strftime("%b")
    dayOfMonth = t.strftime('%d').lstrip('0')
    year = t.strftime("%Y")
    hour = t.strftime('%I').lstrip('0')
    minute = t.strftime('%M')
    ampm = t.strftime('%p').lower()
    return '{dayOfWeek}, {monthName} {dayOfMonth}, {year} {hour}:{minute}{ampm}'.format(dayOfWeek=dayOfWeek, monthName=monthName, dayOfMonth=dayOfMonth, year=year, hour=hour, minute=minute, ampm=ampm)


def stripLeadingArticles(title):
    # given a show title, strip any leading 'The '
    if title.lower().startswith('the '):
        return title[4:]
    return title


class RestServer:
    def __init__(self, db, fileLocations, restServerURL):
        self.db = db
        self.fileLocations = fileLocations
        self.restServerURL = restServerURL

    def makeURL(self, endpoint):
        return self.restServerURL + endpoint

    def rokufyShowData(self, showData):
        rokuData = {}
        rokuData['title'] = showData['name']
        rokuData['description'] = ' '
        if showData['imageURL'] is not None:
            rokuData['hd_img'] = showData['imageURL']
        rokuData['new_episode_list_url'] = self.makeURL('/shows/{}/episodes/new'.format(showData['showID']))
        rokuData['rerun_episode_list_url'] = self.makeURL('/shows/{}/episodes/rerun'.format(showData['showID']))
        rokuData['archived_episode_list_url'] = self.makeURL('/shows/{}/episodes/archive'.format(showData['showID']))
        return rokuData


    def rokufyEpisodeData(self, episodeData):
        rokuData = {}
        rokuData['short_description_1'] = '{epNumber}: {epTitle}'.format(epNumber=episodeData['episodeNumber'], epTitle=episodeData['episodeTitle'])
#        rokuData['short_description_2'] = ' '
        rokuData['description'] = episodeData['episodeDescription']
        if episodeData['imageURL'] is not None:
            rokuData['hd_img'] = episodeData['imageURL']
        elif episodeData['showImageURL'] is not None:
            rokuData['hd_img'] = episodeData['showImageURL']
        rokuData['springboard_url'] = self.makeURL('/recordings/{}'.format(episodeData['recordingID']))
        return rokuData


    def rokufyRecordingData(self, recordingData):
        springboard = {}
        springboard['title'] = recordingData['showName']
        springboard['description'] = recordingData['episodeDescription']
        if recordingData['imageURL'] is not None:
            springboard['hd_img'] = recordingData['imageURL']
        springboard['hd_bif_url'] = recordingData['bifURL']
        springboard['date_recorded'] = formatTime(recordingData['dateRecorded'])
        springboard['length'] = recordingData['duration'].total_seconds()
        springboard['trintv_episode_number'] = '{epNumber}: {epTitle}'.format(epNumber=recordingData['episodeNumber'], epTitle=recordingData['episodeTitle'])
        springboard['trintv_showname'] = recordingData['showName']
        springboard['trintv_delete_url'] = self.makeURL('/recordings/{}'.format(recordingData['recordingID']))
        springboard['trintv_setposition_url'] = self.makeURL('/recordings/{}/playbackPosition/'.format(recordingData['recordingID']))
        springboard['trintv_getposition_url'] = self.makeURL('/recordings/{}/playbackPosition'.format(recordingData['recordingID']))
        springboard['trintv_archive_url'] = self.makeURL('/recordings/{}/archiveState/1'.format(recordingData['recordingID']))
        springboard['trintv_getarchivestate_url'] = self.makeURL('/recordings/{}/archiveState'.format(recordingData['recordingID']))
        springboard['stream'] = { 'format' : 'mp4',
                                  'quality' : 'HD',
                                  'bitrate' : 1000,
                                  'url' : recordingData['transcodedVideoURL']
                                }
        return springboard


    def getAllShows(self):
        showList = self.db.getShowsWithRecordings(['N', 'R', 'A'])
        rokuList = [self.rokufyShowData(show) for show in showList]
        rokuList.sort(key=lambda show: stripLeadingArticles(show['title']))
        return listToRokuXml('shows', 'show', rokuList)

    def getShowsWithNewEpisodes(self):
        showList = self.db.getShowsWithRecordings(['N'])
        rokuList = [self.rokufyShowData(show) for show in showList]
        rokuList.sort(key=lambda show: stripLeadingArticles(show['title']))
        return listToRokuXml('shows', 'show', rokuList)

    def getShowEpisodesNew(self, showID):
        episodeList = self.db.getEpisodeData(showID, ['N'])
        rokuList = [self.rokufyEpisodeData(episode) for episode in episodeList]
        return listToRokuXml('shows', 'show', rokuList)

    def getShowEpisodesRerun(self, showID):
        episodeList = self.db.getEpisodeData(showID, ['R'])
        rokuList = [self.rokufyEpisodeData(episode) for episode in episodeList]
        return listToRokuXml('shows', 'show', rokuList)

    def getShowEpisodesArchive(self, showID):
        episodeList = self.db.getEpisodeData(showID, ['A'])
        rokuList = [self.rokufyEpisodeData(episode) for episode in episodeList]
        xml = listToRokuXml('shows', 'show', rokuList)
        return xml

    def getRecording(self, recordingID):
        recordingData = self.db.getRecordingData(recordingID)
        transcodedVideoLocationID = self.db.getTranscodedVideoLocationID(recordingID)
        recordingData['transcodedVideoURL'] = self.fileLocations.getTranscodedVideoURL(locationID = transcodedVideoLocationID, recordingID = recordingID)
        bifLocationID = self.db.getBifLocationID(recordingID)
        recordingData['bifURL'] = self.fileLocations.getBifURL(locationID = bifLocationID, recordingID = recordingID)
        rokuData = self.rokufyRecordingData(recordingData)
        return listToRokuXml('springboard', 'show', [rokuData])

    def deleteRecording(self, recordingID):
        self.db.deleteRecording(recordingID)
        return str(), 200

    def getPlaybackPosition(self, recordingID):
        return str(self.db.getPlaybackPosition(recordingID)['playbackPosition'])

    def setPlaybackPosition(self, recordingID, playbackPosition):
        self.db.setPlaybackPosition(recordingID, playbackPosition)
        return str(), 200

    def getArchiveState(self, recordingID):
        categoryCode = self.db.getCategoryCode(recordingID)
        if categoryCode == 'A':
            return '1', 200
        else:
            return '0', 200

    def archiveRecording(self, recordingID):
        self.db.setCategoryCode(recordingID, 'A')
        return str(), 200

    def getAlarms(self):
        alarmList = []
        remainingListingTime = self.db.getRemainingListingTime()
        if remainingListingTime.days < 10:
            alarmList.append('Only {} days of listings remaining'.format(remainingListingTime.days))
        if not alarmList and datetime.datetime.now().date().day == 1:
            alarmList.append('Regularly scheduled test alarm (no actual alarms)')
        return alarmList

