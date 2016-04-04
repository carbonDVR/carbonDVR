#!/usr/bin/env python3.4

import json
import os
import psycopg2
import tzlocal
from xml.sax import saxutils

from psycopg2.extensions import register_type, UNICODE
register_type(UNICODE)


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
    dayOfMonth = t.strftime('%d').lstrip('0')
    hour = t.strftime('%I').lstrip('0')
    ampm = t.strftime('%p').lower()
    return '{date1} {dayOfMonth} {hour}:{min}{ampm}'.format(date1=t.strftime('%a, %b'), dayOfMonth=dayOfMonth, hour=hour, min=t.strftime('%M'), ampm=ampm)


def stripLeadingArticles(title):
    # given a show title, strip any leading 'The '
    if title.lower().startswith('the '):
        return title[4:]
    return title


class RestServer:
    def __init__(self, dbConnection, restServerURL, streamURL, bifURL):
        self.dbConnection = dbConnection
        self.restServerURL = restServerURL
        self.streamURL = streamURL
        self.bifURL = bifURL

    def makeURL(self, endpoint):
        return self.restServerURL + endpoint

    def dbGetShowsWithRecordings(self, categoryCodes):
        shows = []
        query = str("SELECT DISTINCT ON (recording.show_id) recording.show_id, show.name, show.imageURL "
                    "FROM recording, show "
                    "WHERE recording.show_id = show.show_id "
                    "AND recording.recording_id IN (SELECT recording_id FROM file_bif) "
                    "AND recording.rerun_code IN %s ;")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query, (tuple(categoryCodes), ))
            for row in cursor:
                shows.append({'showID':row[0], 'name':row[1], 'imageURL':row[2]})
        self.dbConnection.commit()
        return shows


    def dbGetEpisodeData(self, showID, categoryCodes):
        recordings = []
        query = str("SELECT recording.recording_id, recording.show_id, substring(recording.episode_id from '[[:digit:]]*'), "
                    "  episode.title, episode.description, episode.imageurl, show.imageURL "
                    "FROM recording "
                    "INNER JOIN file_transcoded_video ON (recording.recording_id = file_transcoded_video.recording_id) "
                    "INNER JOIN file_bif ON (recording.recording_id = file_bif.recording_id) "
                    "INNER JOIN episode ON (recording.show_id = episode.show_id AND recording.episode_id = episode.episode_id) "
                    "INNER JOIN show ON (recording.show_id = show.show_id) "
                    "WHERE file_transcoded_video.state = 0 "
                    "AND recording.show_id = %s "
                    "AND recording.rerun_code IN %s "
                    "ORDER BY substring(recording.episode_id from '[[:digit:]]*')::integer;")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query, (showID, tuple(categoryCodes)))
            for row in cursor:
                episodeTitle = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeDescription = row[4].encode('ascii', 'xmlcharrefreplace').decode('ascii')     # compensate for Python's inability to cope with unicode
                recordings.append({'recordingID':row[0], 'showID':row[1], 'episodeID':row[2], 'episodeTitle':episodeTitle, 'episodeDescription':episodeDescription, 'imageURL':row[5], 'showImageURL':row[6], 'episodeNumber':row[2]})
        self.dbConnection.commit()
        return recordings


    def dbGetRecordingData(self, recordingID):
        recordingData = None
        query = str("SELECT recording.recording_id, show.name, show.imageurl, episode.title, episode.description, (recording.date_recorded), "
                    "  recording.duration, substring(episode.episode_id from '[[:digit:]]*') "
                    "FROM recording, show, episode "
                    "WHERE recording.show_id = show.show_id "
                    "AND recording.show_id = episode.show_id "
                    "AND recording.episode_id = episode.episode_id "
                    "AND recording_id = %s;")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query, (recordingID, ))
            row = cursor.fetchone()
            if row:
                recordingData = {}
                showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')               # compensate for Python's inability to cope with unicode
                showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')               # compensate for Python's inability to cope with unicode
                episodeTitle = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeDescription = row[4].encode('ascii', 'xmlcharrefreplace').decode('ascii')     # compensate for Python's inability to cope with unicode
                dateRecorded = row[5].astimezone(tzlocal.get_localzone())
                recordingData = {'recordingID':row[0], 'showName':showName, 'imageURL':row[2], 'episodeTitle':episodeTitle, 'episodeDescription':episodeDescription, 'dateRecorded':dateRecorder, 'duration':row[6], 'episodeNumber':row[7]}
        self.dbConnection.commit()
        return recordingData


    def dbDeleteRecording(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM recording WHERE recording_id = %s;', (recordingID, ))
        self.dbConnection.commit()

    def dbSetPlaybackPosition(self, recordingID, playbackPosition):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('UPDATE playback_position SET position = %s WHERE recording_id = %s;', (playbackPosition, recordingID))
            if cursor.rowcount == 0:
                cursor.execute('INSERT INTO playback_position (recording_id, position) VALUES (%s, %s);', (recordingID, playbackPosition))
        self.dbConnection.commit()

    def dbGetPlaybackPosition(self, recordingID):
        playbackPosition = 0
        with self.dbConnection.cursor() as cursor:
            cursor.execute('SELECT position FROM playback_position WHERE recording_id = %s;', (recordingID, ))
            row = cursor.fetchone()
            if row:
                playbackPosition = row[0]
        self.dbConnection.commit()
        return {'playbackPosition': playbackPosition}

    def dbSetCategoryCode(self, recordingID, categoryCode):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('UPDATE recording SET rerun_code = %s WHERE recording_id = %s;', (categoryCode, recordingID))
        self.dbConnection.commit()

    def dbGetCategoryCode(self, recordingID):
        categoryCode = ''
        with self.dbConnection.cursor() as cursor:
            cursor.execute('SELECT rerun_code FROM recording WHERE recording_id = %s;', (recordingID, ))
            row = cursor.fetchone()
            if row:
                categoryCode = row[0]
        self.dbConnection.commit()
        return categoryCode

    def dbRemainingListingTime(self):
        with self.dbConnection:
            with self.dbConnection.cursor() as cursor:
                cursor.execute('SELECT max(start_time) - now() FROM schedule;')
                row = cursor.fetchone()
                return row[0]


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
        springboard['hd_bif_url'] = self.bifURL.format(recordingID = recordingData['recordingID'])
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
                                  'url' : self.streamURL.format(recordingID = recordingData['recordingID'])
                                }
        return springboard


    def getAllShows(self):
        showList = self.dbGetShowsWithRecordings(['N', 'R', 'A'])
        rokuList = [self.rokufyShowData(show) for show in showList]
        rokuList.sort(key=lambda show: stripLeadingArticles(show['title']))
        return listToRokuXml('shows', 'show', rokuList)

    def getShowsWithNewEpisodes(self):
        showList = self.dbGetShowsWithRecordings(['N'])
        rokuList = [self.rokufyShowData(show) for show in showList]
        rokuList.sort(key=lambda show: stripLeadingArticles(show['title']))
        return listToRokuXml('shows', 'show', rokuList)

    def getShowEpisodesNew(self, showID):
        episodeList = self.dbGetEpisodeData(showID, ['N'])
        rokuList = [self.rokufyEpisodeData(episode) for episode in episodeList]
        return listToRokuXml('shows', 'show', rokuList)

    def getShowEpisodesRerun(self, showID):
        episodeList = self.dbGetEpisodeData(showID, ['R'])
        rokuList = [self.rokufyEpisodeData(episode) for episode in episodeList]
        return listToRokuXml('shows', 'show', rokuList)

    def getShowEpisodesArchive(self, showID):
        episodeList = self.dbGetEpisodeData(showID, ['A'])
        rokuList = [self.rokufyEpisodeData(episode) for episode in episodeList]
        xml = listToRokuXml('shows', 'show', rokuList)
        return xml

    def getRecording(self, recordingID):
        recordingData = self.dbGetRecordingData(recordingID)
        rokuData = self.rokufyRecordingData(recordingData)
        return listToRokuXml('springboard', 'show', [rokuData])

    def deleteRecording(self, recordingID):
        self.dbDeleteRecording(recordingID)
        return str(), 200

    def getPlaybackPosition(self, recordingID):
        return str(self.dbGetPlaybackPosition(recordingID)['playbackPosition'])

    def setPlaybackPosition(self, recordingID, playbackPosition):
        self.dbSetPlaybackPosition(recordingID, playbackPosition)
        return str(), 200

    def getArchiveState(self, recordingID):
        categoryCode = self.dbGetCategoryCode(recordingID)
        if categoryCode == 'A':
            return '1', 200
        else:
            return '0', 200

    def archiveRecording(self, recordingID):
        self.dbSetCategoryCode(recordingID, 'A')
        return str(), 200

    def getAlarms(self):
        alarmList = []
        remainingListingTime = self.dbRemainingListingTime()
        if remainingListingTime.days < 20:
            alarmList.append('Only {} days of listings remaining'.format(remainingListingTime.days))
        return alarmList

