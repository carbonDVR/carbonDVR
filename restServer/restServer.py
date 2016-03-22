#!/usr/bin/env python3.4

import configparser
from flask import Flask, g, abort, json, render_template, redirect, request, url_for, current_app
import os
import psycopg2
from xml.sax import saxutils

from psycopg2.extensions import register_type, UNICODE
register_type(UNICODE)


class Bunch():
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class globalConfig:
    pass


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




def dbGetShowsWithRecordings(dbConnection, categoryCodes):
    shows = []
    cursor = dbConnection.cursor()
    query = str("SELECT DISTINCT ON (recording.show_id) recording.show_id, show.name, show.imageURL "
                "FROM recording, show "
                "WHERE recording.show_id = show.show_id "
                "AND recording.recording_id IN (SELECT recording_id FROM file_bif) "
                "AND recording.rerun_code IN %s ;")
    cursor.execute(query, (tuple(categoryCodes), ))
    for row in cursor:
        shows.append({'showID':row[0], 'name':row[1], 'imageURL':row[2]})
    return shows


def dbGetEpisodeData(dbConnection, showID, categoryCodes):
    recordings = []
    cursor = dbConnection.cursor()
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
    cursor.execute(query, (showID, tuple(categoryCodes)))
    for row in cursor:
        episodeTitle = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
        episodeDescription = row[4].encode('ascii', 'xmlcharrefreplace').decode('ascii')     # compensate for Python's inability to cope with unicode
        recordings.append({'recordingID':row[0], 'showID':row[1], 'episodeID':row[2], 'episodeTitle':episodeTitle, 'episodeDescription':episodeDescription, 'imageURL':row[5], 'showImageURL':row[6], 'episodeNumber':row[2]})
    return recordings


def dbGetRecordingData(dbConnection, recordingID):
    cursor = dbConnection.cursor()
    query = str("SELECT recording.recording_id, show.name, show.imageurl, episode.title, episode.description, (recording.date_recorded), "
                "  recording.duration, substring(episode.episode_id from '[[:digit:]]*') "
                "FROM recording, show, episode "
                "WHERE recording.show_id = show.show_id "
                "AND recording.show_id = episode.show_id "
                "AND recording.episode_id = episode.episode_id "
                "AND recording_id = %s;")
    cursor.execute(query, (recordingID, ))
    row = cursor.fetchone()
    if row == None:
        return None
    showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')               # compensate for Python's inability to cope with unicode
    showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')               # compensate for Python's inability to cope with unicode
    episodeTitle = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
    episodeDescription = row[4].encode('ascii', 'xmlcharrefreplace').decode('ascii')     # compensate for Python's inability to cope with unicode
    return {'recordingID':row[0], 'showName':showName, 'imageURL':row[2], 'episodeTitle':episodeTitle, 'episodeDescription':episodeDescription, 'dateRecorded':row[5], 'duration':row[6], 'episodeNumber':row[7]}


def dbDeleteRecording(dbConnection, recordingID):
    cursor = dbConnection.cursor();
    cursor.execute('DELETE FROM recording WHERE recording_id = %s;', (recordingID, ))
    dbConnection.commit()
    cursor.close()

def dbSetPlaybackPosition(dbConnection, recordingID, playbackPosition):
    cursor = dbConnection.cursor();
    cursor.execute('UPDATE playback_position SET position = %s WHERE recording_id = %s;', (playbackPosition, recordingID))
    if cursor.rowcount == 0:
        cursor.execute('INSERT INTO playback_position (recording_id, position) VALUES (%s, %s);', (recordingID, playbackPosition))
    dbConnection.commit()
    cursor.close()

def dbGetPlaybackPosition(dbConnection, recordingID):
    cursor = dbConnection.cursor();
    cursor.execute('SELECT position FROM playback_position WHERE recording_id = %s;', (recordingID, ))
    row = cursor.fetchone()
    cursor.close()
    if row is None:
        return {'playbackPosition': 0 }
    else:
        return {'playbackPosition': row[0] }

def dbSetCategoryCode(dbConnection, recordingID, categoryCode):
    cursor = dbConnection.cursor();
    cursor.execute('UPDATE recording SET rerun_code = %s WHERE recording_id = %s;', (categoryCode, recordingID))
    dbConnection.commit()
    cursor.close()

def dbGetCategoryCode(dbConnection, recordingID):
    cursor = dbConnection.cursor();
    cursor.execute('SELECT rerun_code FROM recording WHERE recording_id = %s;', (recordingID, ))
    row = cursor.fetchone()
    cursor.close()
    if row is None:
        return ''
    else:
        return row[0]

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


def rokufyShowData(showData):
    rokuData = {}
    rokuData['title'] = showData['name']
    rokuData['description'] = ' '
    rokuData['sd_img'] = globalConfig.genericSDPosterURL
    rokuData['hd_img'] = showData['imageURL']
    if rokuData['hd_img'] is None:
        rokuData['hd_img'] = globalConfig.genericHDPosterURL
    rokuData['new_episode_list_url'] = url_for('getShowEpisodesNew', showID = showData['showID'], _external=True)
    rokuData['rerun_episode_list_url'] = url_for('getShowEpisodesRerun', showID = showData['showID'], _external=True)
    rokuData['archived_episode_list_url'] = url_for('getShowEpisodesArchive', showID = showData['showID'], _external=True)
    return rokuData


def rokufyEpisodeData(episodeData):
    rokuData = {}
    rokuData['short_description_1'] = '{epNumber}: {epTitle}'.format(epNumber=episodeData['episodeNumber'], epTitle=episodeData['episodeTitle'])
#    rokuData['short_description_2'] = ' '
    rokuData['description'] = episodeData['episodeDescription']
    rokuData['sd_img'] = globalConfig.genericSDPosterURL
    rokuData['hd_img'] = episodeData['imageURL']
    if rokuData['hd_img'] is None:
        rokuData['hd_img'] = episodeData['showImageURL']
    if rokuData['hd_img'] is None:
        rokuData['hd_img'] = globalConfig.genericHDPosterURL
    rokuData['springboard_url'] = url_for('getRecording', recordingID = episodeData['recordingID'], _external=True)
    return rokuData


def rokufyRecordingData(recordingData):
    springboard = {}
    springboard['title'] = recordingData['showName']
    springboard['description'] = recordingData['episodeDescription']
    springboard['sd_img'] = globalConfig.genericSDPosterURL
    springboard['hd_img'] = recordingData['imageURL']
    if springboard['hd_img'] is None:
        springboard['hd_img'] = globalConfig.genericHDPosterURL
    springboard['hd_bif_url'] = globalConfig.bifURL.format(recordingID = recordingData['recordingID'])
    springboard['date_recorded'] = formatTime(recordingData['dateRecorded'])
    springboard['length'] = recordingData['duration'].total_seconds()
    springboard['trintv_episode_number'] = '{epNumber}: {epTitle}'.format(epNumber=recordingData['episodeNumber'], epTitle=recordingData['episodeTitle'])
    springboard['trintv_showname'] = recordingData['showName']
    springboard['trintv_delete_url'] = url_for('deleteRecording', recordingID = recordingData['recordingID'], _external=True)
    springboard['trintv_setposition_url'] = url_for('getPlaybackPosition', recordingID = recordingData['recordingID'], _external=True) + '/'
    springboard['trintv_getposition_url'] = url_for('getPlaybackPosition', recordingID = recordingData['recordingID'], _external=True)
    springboard['trintv_archive_url'] = url_for('archiveRecording', recordingID = recordingData['recordingID'], _external=True)
    springboard['trintv_getarchivestate_url'] = url_for('getArchiveState', recordingID = recordingData['recordingID'], _external=True)
    springboard['stream'] = { 'format' : 'mp4',
                              'quality' : 'HD',
                              'bitrate' : 1000,
                              'url' : globalConfig.streamURL.format(recordingID = recordingData['recordingID'])
                            }
    return springboard





app = Flask(__name__)

@app.route('/shows')
def getAllShows():
    showList = dbGetShowsWithRecordings(current_app.dbConnection, ['N', 'R', 'A'])
    rokuList = [rokufyShowData(show) for show in showList]
    rokuList.sort(key=lambda show: stripLeadingArticles(show['title']))
    return listToRokuXml('shows', 'show', rokuList)

@app.route('/shows/new')
def getShowsWithNewEpisodes():
    showList = dbGetShowsWithRecordings(current_app.dbConnection, ['N'])
    rokuList = [rokufyShowData(show) for show in showList]
    rokuList.sort(key=lambda show: stripLeadingArticles(show['title']))
    return listToRokuXml('shows', 'show', rokuList)

@app.route('/shows/<showID>/episodes/new')
def getShowEpisodesNew(showID):
    episodeList = dbGetEpisodeData(current_app.dbConnection, showID, ['N'])
    rokuList = [rokufyEpisodeData(episode) for episode in episodeList]
    return listToRokuXml('shows', 'show', rokuList)

@app.route('/shows/<showID>/episodes/rerun')
def getShowEpisodesRerun(showID):
    episodeList = dbGetEpisodeData(current_app.dbConnection, showID, ['R'])
    rokuList = [rokufyEpisodeData(episode) for episode in episodeList]
    return listToRokuXml('shows', 'show', rokuList)

@app.route('/shows/<showID>/episodes/archive')
def getShowEpisodesArchive(showID):
    episodeList = dbGetEpisodeData(current_app.dbConnection, showID, ['A'])
    rokuList = [rokufyEpisodeData(episode) for episode in episodeList]
    xml = listToRokuXml('shows', 'show', rokuList)
    return xml

@app.route('/recordings/<recordingID>')
def getRecording(recordingID):
    recordingData = dbGetRecordingData(current_app.dbConnection, recordingID)
    rokuData = rokufyRecordingData(recordingData)
    return listToRokuXml('springboard', 'show', [rokuData])

@app.route('/recordings/<recordingID>', methods=['DELETE'])
def deleteRecording(recordingID):
    dbDeleteRecording(current_app.dbConnection, recordingID)
    return str(), 200

@app.route('/recordings/<recordingID>/playbackPosition')
def getPlaybackPosition(recordingID):
    return str(dbGetPlaybackPosition(current_app.dbConnection, recordingID)['playbackPosition'])

@app.route('/recordings/<recordingID>/playbackPosition/<playbackPosition>', methods=['PUT'])
def setPlaybackPosition(recordingID, playbackPosition):
    dbSetPlaybackPosition(current_app.dbConnection, recordingID, playbackPosition)
    return str(), 200

@app.route('/recordings/<recordingID>/archiveState')
def getArchiveState(recordingID):
    categoryCode = dbGetCategoryCode(current_app.dbConnection, recordingID)
    if categoryCode == 'A':
        return '1', 200
    else:
        return '0', 200

@app.route('/recordings/<recordingID>/archiveState/1', methods=['PUT'])
def archiveRecording(recordingID):
    dbSetCategoryCode(current_app.dbConnection, recordingID, 'A')
    return str(), 200

