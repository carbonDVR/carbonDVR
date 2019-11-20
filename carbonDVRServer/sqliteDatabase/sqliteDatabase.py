#!/usr/bin/env python3

import argparse
from datetime import datetime, timedelta, timezone
import re
import threading
from threading import current_thread

from bunch import Bunch
import sqlite3


threadLocal = threading.local()


def fromDatetime(datetimeValue):
    if datetimeValue.tzinfo is None:
        datetimeValue = datetimeValue.replace(tzinfo=timezone.utc)
    return datetimeValue.strftime("%Y-%m-%dT%H:%M:%S%z")

def toDatetime(fieldValue):
    return datetime.strptime(fieldValue, "%Y-%m-%dT%H:%M:%S%z")

def fromDuration(durationValue):
    return durationValue.total_seconds()

def toDuration(fieldValue):
    return timedelta(seconds=fieldValue)

# some entries in the listing data use unicode
# converting to ascii saves some headache
def toAscii(fieldValue):
    return fieldValue.encode('ascii', 'xmlcharrefreplace').decode('ascii')

# the episodeID may contain non-numeric values (e.g. "57_1", indicating part 1 of episode 57)
# extract the leading digits and treat those as the episode number
def toEpisodeNumber(episodeID):
    return int(re.match('\d*', episodeID).group(0))


class SqliteDatabase:
    TRANSCODE_SUCCESSFUL = 0
    TRANSCODE_FAILED = 1

    def __init__(self, dbFile):
        self.dbFile = dbFile
        self.initializeSchema()

    def __del__(self):
        self.getConnection().close()
        threadLocal.connection = None

    # sqlite only allows connections to be used in the same thread in which they were opened
    # use thread local storage to store sqlite connections
    def getConnection(self):
        connection = getattr(threadLocal, 'connection', None)
        if connection is not None:
            return connection
        connection = sqlite3.connect(self.dbFile, isolation_level=None)
        threadLocal.connection = connection
        return connection

    def initializeSchema(self):
        if self.getSchemaVersion() != 0:
          return
        schemaScript = '''
        CREATE TABLE schema_version (
            version integer);
        CREATE TABLE uniqueid (
            nextID integer);
        CREATE TABLE show (
            show_id        text PRIMARY KEY,
            show_type      text,
            name           text,
            imageurl       text);
        CREATE TABLE episode (
            show_id        text,
            episode_id     text,
            title          text,
            description    text,
            part_code      text,
            imageurl       text,
            PRIMARY KEY (show_id, episode_id),
            FOREIGN KEY (show_id) REFERENCES show(show_id));
        CREATE TABLE channel (
            major          integer,
            minor          integer,
            actual         integer,
            program        integer,
            PRIMARY KEY (major, minor));
        CREATE TABLE tuner (
            device_id      text,
            ipaddress      text,
            tuner_id       integer);
        CREATE TABLE schedule (
            schedule_id    INTEGER PRIMARY KEY,
            channel_major  integer,
            channel_minor  integer,
            start_time     text,
            duration       integer,
            show_id        text,
            episode_id     text,
            rerun_code     text,
            FOREIGN KEY (channel_major, channel_minor) REFERENCES channel(major, minor),
            FOREIGN KEY (show_id, episode_id) REFERENCES episode(show_id, episode_id));
        CREATE TABLE subscription (
            show_id        text PRIMARY KEY,
            priority       integer);
        CREATE TABLE recording_state (
            state          integer,
            description    text);
        CREATE TABLE recording (
            recording_id   integer PRIMARY KEY,
            show_id        text,
            episode_id     text,
            date_recorded  integer,
            duration       integer,
            category_code text,
            FOREIGN KEY (show_id, episode_id) REFERENCES episode(show_id, episode_id));
        CREATE TABLE file_raw_video (
            recording_id   integer PRIMARY KEY,
            filename       text);
        CREATE TABLE file_transcoded_video (
            recording_id   integer PRIMARY KEY,
            filename       text,
            state          int,
            location_id    int);
        CREATE TABLE file_bif (
            recording_id   integer PRIMARY KEY,
            location_id    int,
            filename       text);
        CREATE TABLE playback_position (
            recording_id   integer PRIMARY KEY,
            position       integer);
        CREATE VIEW recorded_episodes_by_id AS
            SELECT recording.recording_id, recording.show_id, recording.episode_id
            FROM recording
            LEFT JOIN file_raw_video ON (recording.recording_id = file_raw_video.recording_id)
            LEFT JOIN file_transcoded_video ON (recording.recording_id = file_transcoded_video.recording_id)
            WHERE file_raw_video.filename IS NOT NULL
            OR file_transcoded_video.filename IS NOT NULL;'''
        self.getConnection().executescript(schemaScript)
        self.getConnection().execute("INSERT INTO schema_version VALUES (1)")
        self.getConnection().execute("INSERT INTO uniqueid VALUES (1)")
        self.getConnection().commit()

    def getSchemaVersion(self):
      try:
        cursor = self.getConnection().execute("SELECT version FROM schema_version");
        if cursor.rowcount == 0:
          raise Exception('[43YWBN] no rows in schema table.'.format(cursor.rowcount))
        if cursor.rowcount > 1:
          raise Exception('[43YWBP] too many rows in schema table.  rowcount={}'.format(cursor.rowcount))
        return cursor.fetchone()[0]
      except sqlite3.OperationalError:
        return 0

    def insertShow(self, showID, showType, name):
        cursor = self.getConnection().execute(
            "INSERT INTO show(show_id, show_type, name) VALUES (?, ?, ?)",
            (showID, showType, name))
        self.getConnection().commit()
        return cursor.rowcount

    def insertShows(self, programs):
        numRowsInserted = 0
        numRowsUpdated = 0
        cursor = self.getConnection().cursor()
        cursor.execute("BEGIN")
        for program in programs:
            cursor.execute("SELECT count(*) FROM show WHERE show_id = ?", (program.showID, ))
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "INSERT INTO show(show_id, show_type, name) VALUES (?, ?, ?)",
                    (program.showID, program.showType, program.showName))
                numRowsInserted += cursor.rowcount
            else:
                cursor.execute(
                  "UPDATE show set show_type = ?, name = ? WHERE show_id = ?",
                  (program.showType, program.showName, program.showID))
                numRowsUpdated += cursor.rowcount
        cursor.execute("COMMIT")
        return numRowsInserted, numRowsUpdated

    def insertSubscription(self, showID, priority):
        cursor = self.getConnection().execute(
            "INSERT INTO subscription(show_id, priority) VALUES (?, ?)",
            (showID, priority))
        self.getConnection().commit()
        return cursor.rowcount

    def deleteSubscription(self, showID):
        cursor = self.getConnection().execute('DELETE FROM subscription WHERE show_id = ?', (showID, ))
        self.getConnection().commit()
        return cursor.rowcount

    def insertEpisode(self, showID, episodeID, title, description):
        cursor = self.getConnection().execute(
            "INSERT INTO episode(show_id, episode_id, title, description) VALUES (?, ?, ?, ?)",
            (showID, episodeID, title, description))
        self.getConnection().commit()
        return cursor.rowcount

    def insertEpisodes(self, programs):
        numRowsInserted = 0
        cursor = self.getConnection().cursor()
        cursor.execute("BEGIN")
        for program in programs:
            cursor.execute("SELECT count(*) FROM episode WHERE show_id = ? AND episode_id = ?", (program.showID, program.episodeID))
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "INSERT INTO episode(show_id, episode_id, title, description) VALUES (?, ?, ?, ?)",
                    (program.showID, program.episodeID, program.episodeTitle, program.episodeDescription))
                numRowsInserted += cursor.rowcount
        cursor.execute("COMMIT")
        return numRowsInserted

    def insertSchedule(self, channelMajor, channelMinor, startTime, duration, showID, episodeID, rerunCode):
        cursor = self.getConnection().execute(
            'INSERT INTO schedule(channel_major, channel_minor, start_time, duration, show_id, episode_id, rerun_code) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (channelMajor, channelMinor, fromDatetime(startTime), fromDuration(duration), showID, episodeID, rerunCode))
        self.getConnection().commit()
        return cursor.rowcount

    def insertSchedules(self, schedules):
        numRowsInserted = 0
        cursor = self.getConnection().cursor()
        cursor.execute("BEGIN")
        for schedule in schedules:
            cursor.execute(
                'INSERT INTO schedule(channel_major, channel_minor, start_time, duration, show_id, episode_id, rerun_code) VALUES (?, ?, ?, ?, ?, ?, ?)',
                 (schedule.channelMajor,
                  schedule.channelMinor,
                  fromDatetime(schedule.startTime),
                  fromDuration(schedule.duration),
                  schedule.showID,
                  schedule.episodeID,
                  schedule.rerunCode))
            numRowsInserted += cursor.rowcount
        cursor.execute("COMMIT")
        return numRowsInserted

    def clearScheduleTable(self):
        numRowsDeleted = 0
        cursor = self.getConnection().cursor()
        cursor.execute("DELETE FROM schedule")
        return cursor.rowcount

    def insertRecording(self, recordingID, showID, episodeID, dateRecorded, duration, categoryCode):
        dateRecordedString = dateRecorded.strftime("%Y-%m-%dT%H:%M:%S%z")
        cursor = self.getConnection().execute(
            "INSERT INTO recording(recording_id, show_id, episode_id, date_recorded, duration, category_code) VALUES (?, ?, ?, ?, ?, ?)",
            (recordingID, showID, episodeID, fromDatetime(dateRecorded), fromDuration(duration), categoryCode))
        self.getConnection().commit()
        return cursor.rowcount

    def insertTuner(self, deviceID, ipaddress, tunerID):
        cursor = self.getConnection().execute(
            "INSERT INTO tuner(device_id, ipaddress, tuner_id) VALUES (?, ?, ?)",
            (deviceID, ipaddress, tunerID))
        self.getConnection().commit()
        return cursor.rowcount

    def insertChannel(self, channelMajor, channelMinor, channelActual, program):
        cursor = self.getConnection().execute(
            "INSERT INTO channel(major, minor, actual, program) VALUES (?, ?, ?, ?)",
            (channelMajor, channelMinor, channelActual, program))
        self.getConnection().commit()
        return cursor.rowcount


    #
    # Functions used by recorder
    #


    def getChannels(self):
        channels = []
        for row in self.getConnection().execute("SELECT major, minor, actual, program FROM channel"):
          channels.append(Bunch(channelMajor=row[0], channelMinor=row[1], channelActual=row[2], program=row[3]))
        return channels

    def getTuners(self):
        tuners = []
        for row in self.getConnection().execute("SELECT device_id, ipaddress, tuner_id FROM tuner"):
          tuners.append(Bunch(deviceID=row[0], ipAddress=row[1], tunerID=row[2]))
        return tuners

    # this is the correct version, which we can't use until we install sqlite version 3.15.0 or higher
    def getPendingRecordings_correct(self):
        query = str("SELECT DISTINCT ON (schedule.show_id, schedule.episode_id) "
                    "schedule.schedule_id, schedule.channel_major, schedule.channel_minor, schedule.start_time, "
                    "schedule.duration, schedule.show_id, schedule.episode_id, schedule.rerun_code "
                    "FROM schedule "
                    "INNER JOIN subscription ON (schedule.show_id = subscription.show_id) "
                    "WHERE schedule.start_time > ? "
                    "AND schedule.start_time < ? "
                    "AND (schedule.show_id, schedule.episode_id) NOT IN "
                        "(SELECT recorded_episodes_by_id.show_id, recorded_episodes_by_id.episode_id FROM recorded_episodes_by_id) "
                    "ORDER BY schedule.show_id, schedule.episode_id, schedule.start_time")
        currentTime = fromDatetime(datetime.utcnow())
        endTime = fromDatetime(datetime.utcnow() + timedelta(hours=12))
        schedules = []
        for row in self.getConnection().execute(query, (currentTime, endTime)):
            duration = toDuration(row[4])
            schedules.append(Bunch(channelMajor=row[1], channelMinor=row[2], startTime=row[3], duration=duration, showID=row[5], episodeID=row[6], rerunCode=row[7]))
        return schedules

    # this is the workaround hack
    def getPendingRecordings(self):
        query = str("SELECT schedule.schedule_id, schedule.channel_major, schedule.channel_minor, schedule.start_time, "
                    "schedule.duration, schedule.show_id, schedule.episode_id, schedule.rerun_code "
                    "FROM schedule "
                    "INNER JOIN subscription ON (schedule.show_id = subscription.show_id) "
                    "WHERE schedule.start_time > ? "
                    "AND schedule.start_time < ? "
                    "AND schedule.show_id || '-' || schedule.episode_id NOT IN "
                        "(SELECT recorded_episodes_by_id.show_id || '-' || recorded_episodes_by_id.episode_id FROM recorded_episodes_by_id) "
                    "ORDER BY schedule.show_id, schedule.episode_id, schedule.start_time")
        currentTime = fromDatetime(datetime.utcnow())
        endTime = fromDatetime(datetime.utcnow() + timedelta(hours=12))
        schedules = []
        for row in self.getConnection().execute(query, (currentTime, endTime)):
            startTime = toDatetime(row[3])
            duration = toDuration(row[4])
            schedules.append(Bunch(channelMajor=row[1], channelMinor=row[2], startTime=startTime, duration=duration, showID=row[5], episodeID=row[6], rerunCode=row[7]))

        # sqlite select statements don't support "DISTINCT ON (column1, column2)", so we have to roll our own
        usedKeys = set()
        prunedSchedules = []
        for schedule in schedules:
            key = (schedule.showID, schedule.episodeID)
            if key not in usedKeys:
                prunedSchedules.append(schedule)
                usedKeys.add(key);
        return prunedSchedules

    def getUniqueID(self):
        self.getConnection().execute("BEGIN TRANSACTION")
        uniqueID = self.getConnection().execute("SELECT nextID FROM uniqueid").fetchone()[0]
        self.getConnection().execute("DELETE FROM uniqueid")
        self.getConnection().execute("INSERT INTO uniqueid VALUES (?)", (uniqueID + 1,))
        self.getConnection().execute("COMMIT")
        return uniqueID

    def insertRawFileLocation(self, recordingID, filename):
        cursor = self.getConnection().execute("INSERT INTO file_raw_video(recording_id, filename) VALUES (?, ?)", (recordingID, filename))
        self.getConnection().commit()
        return cursor.rowcount


    #
    # Functions used by transcoder
    #


    def insertTranscodedFileLocation(self, recordingID, locationID, filename, state):
        cursor = self.getConnection().execute(
            "INSERT INTO file_transcoded_video(recording_id, location_id, filename, state) VALUES (?, ?, ?, ?)",
            (recordingID, locationID, filename, state))
        self.getConnection().commit()
        return cursor.rowcount

    def getRecordingsToBif(self):
        cursor = self.getConnection().execute(
            "SELECT recording_id, filename FROM file_transcoded_video WHERE state = 0 AND recording_id NOT IN (SELECT recording_id FROM file_bif)")
        recordings = []
        for row in cursor:
            recordings.append(Bunch(recordingID=row[0], filename=row[1]))
        self.getConnection().commit()
        return recordings

    def insertBifFileLocation(self, recordingID, locationID, filename):
        cursor = self.getConnection().execute(
            "INSERT INTO file_bif(recording_id, location_id, filename) VALUES (?, ?, ?)",
            (recordingID, locationID, filename))
        self.getConnection().commit()
        return cursor.rowcount

    def getUnreferencedRawVideoRecords(self):
        query = str('SELECT recording_id, filename '
                    'FROM file_raw_video '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id')
        records = []
        for row in self.getConnection().execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def deleteRawVideoRecord(self, recordingID):
        cursor = self.getConnection().execute('DELETE FROM file_raw_video WHERE recording_id = ?', (recordingID, ))
        self.getConnection().commit()
        return cursor.rowcount

    def getUnreferencedTranscodedVideoRecords(self):
        query = str('SELECT recording_id, filename '
                    'FROM file_transcoded_video '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id')
        records = []
        for row in self.getConnection().execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def deleteTranscodedVideoRecord(self, recordingID):
        cursor = self.getConnection().execute('DELETE FROM file_transcoded_video WHERE recording_id = ?', (recordingID, ))
        self.getConnection().commit()
        return cursor.rowcount

    def getUnreferencedBifRecords(self):
        query = str('SELECT recording_id, filename '
                    'FROM file_bif '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id')
        records = []
        for row in self.getConnection().execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def deleteBifRecord(self, recordingID):
        cursor = self.getConnection().execute('DELETE FROM file_bif WHERE recording_id = ?', (recordingID, ))
        self.getConnection().commit()
        return cursor.rowcount

    def getUnneededRawVideoRecords(self):
        query = str('SELECT file_raw_video.recording_id, file_raw_video.filename '
                    'FROM file_raw_video '
                    'INNER JOIN file_transcoded_video USING (recording_id) '
                    'WHERE file_transcoded_video.state = 0 '
                    'ORDER BY file_raw_video.recording_id')
        records = []
        for row in self.getConnection().execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def selectRecordingsToTranscode(self):
        query = "SELECT recording_id, filename FROM file_raw_video WHERE recording_id NOT IN (SELECT recording_id FROM file_transcoded_video)"
        recordings = []
        for row in self.getConnection().execute(query):
           recordings.append(Bunch(recordingID=row[0], filename=row[1]))
        return recordings

    def getDuration(self, recordingID):
        cursor = self.getConnection().execute("SELECT duration FROM recording WHERE recording_id = ?", (recordingID,))
        row = cursor.fetchone()
        if not row :
            return timedelta(seconds=0)
        return toDuration(row[0])


    #
    # Functions used by REST server
    #


    def getShowsWithRecordings(self, categoryCodes):
        query = str("SELECT DISTINCT recording.show_id, show.name, show.imageURL "
                    "FROM recording, show "
                    "WHERE recording.show_id = show.show_id "
                    "AND recording.recording_id IN (SELECT recording_id FROM file_bif) "
                    "AND recording.category_code = ?"
                    "ORDER BY show.name")
        shows = []
        for categoryCode in categoryCodes:
          for row in self.getConnection().execute(query, (categoryCode,)):
            shows.append({'showID':row[0], 'name':row[1], 'imageURL':row[2]})
        return shows

    def getEpisodeData(self, showID, categoryCodes):
        query = str("SELECT recording.recording_id, recording.show_id, recording.episode_id, "
                    "  episode.title, episode.description, episode.imageurl, show.imageURL "
                    "FROM recording "
                    "INNER JOIN file_transcoded_video ON (recording.recording_id = file_transcoded_video.recording_id) "
                    "INNER JOIN file_bif ON (recording.recording_id = file_bif.recording_id) "
                    "INNER JOIN episode ON (recording.show_id = episode.show_id AND recording.episode_id = episode.episode_id) "
                    "INNER JOIN show ON (recording.show_id = show.show_id) "
                    "WHERE file_transcoded_video.state = 0 "
                    "AND recording.show_id = ? "
                    "AND recording.category_code = ? ")
        recordings = []
        for categoryCode in categoryCodes:
            for row in self.getConnection().execute(query, (showID, categoryCode)):
                episodeNumber = toEpisodeNumber(row[2])
                episodeTitle = toAscii(row[3])
                episodeDescription = toAscii(row[4])
                recordings.append({
                    'recordingID':row[0],
                    'showID':row[1],
                    'episodeID':row[2],
                    'episodeTitle':episodeTitle,
                    'episodeDescription':episodeDescription,
                    'imageURL':row[5],
                    'showImageURL':row[6],
                    'episodeNumber':episodeNumber})
        recordings.sort(key=lambda x:x['episodeNumber'])
        return recordings

    def getRecordingData(self, recordingID):
        query = str("SELECT recording.recording_id, show.name, show.imageurl, episode.title, episode.description, episode.episode_id, "
                    "recording.date_recorded, recording.duration "
                    "FROM recording, show, episode "
                    "WHERE recording.show_id = show.show_id "
                    "AND recording.show_id = episode.show_id "
                    "AND recording.episode_id = episode.episode_id "
                    "AND recording.recording_id = ?")
        cursor = self.getConnection().execute(query, (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return None
        showName = toAscii(row[1])
        episodeTitle = toAscii(row[3])
        episodeDescription = toAscii(row[4])
        episodeNumber = toEpisodeNumber(row[5])
        dateRecorded = toDatetime(row[6])
        duration = toDuration(row[7])
        recordingData = {'recordingID':row[0], 'showName':showName, 'imageURL':row[2], 'episodeTitle':episodeTitle, 'episodeDescription':episodeDescription, 'dateRecorded':dateRecorded, 'duration':duration, 'episodeNumber':episodeNumber}
        return recordingData

    def getTranscodedVideoLocationID(self, recordingID):
        cursor = self.getConnection().execute('SELECT location_id FROM file_transcoded_video WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return 0
        return row[0]

    def getBifLocationID(self, recordingID):
        cursor = self.getConnection().execute('SELECT location_id FROM file_bif WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
            return 0
        return row[0]

    def deleteRecording(self, recordingID):
        cursor = self.getConnection().execute('DELETE FROM recording WHERE recording_id = ?', (recordingID, ))
        self.getConnection().commit()
        return cursor.rowcount

    def setPlaybackPosition(self, recordingID, playbackPosition):
        cursor = self.getConnection().execute('UPDATE playback_position SET position = ? WHERE recording_id = ?', (playbackPosition, recordingID))
        if cursor.rowcount == 0:
            cursor = self.getConnection().execute('INSERT INTO playback_position (recording_id, position) VALUES (?, ?)', (recordingID, playbackPosition))
        self.getConnection().commit()
        return cursor.rowcount

    def getPlaybackPosition(self, recordingID):
        cursor = self.getConnection().execute('SELECT position FROM playback_position WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return {'playbackPosition': 0}
        return {'playbackPosition': row[0]}

    def setCategoryCode(self, recordingID, categoryCode):
        cursor = self.getConnection().execute('UPDATE recording SET category_code = ? WHERE recording_id = ?', (categoryCode, recordingID))
        self.getConnection().commit()
        return cursor.rowcount

    def getCategoryCode(self, recordingID):
        cursor = self.getConnection().execute('SELECT category_code FROM recording WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return ''
        return row[0]

    def getRemainingListingTime(self):
        cursor = self.getConnection().execute('SELECT max(start_time) FROM schedule')
        row = cursor.fetchone()
        if not row:
          return timedelta(seconds=0)
        latestListing = toDatetime(row[0])
        return latestListing - datetime.utcnow().replace(tzinfo=timezone.utc)


    #
    # Functions used by UI Server
    #


    def getAllRecordings(self):
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded, recording.duration "
                    "FROM recording "
                    "INNER JOIN show ON (recording.show_id = show.show_id) "
                    "INNER JOIN episode ON (recording.show_id = episode.show_id AND recording.episode_id = episode.episode_id) "
                    "WHERE recording.recording_id IN (SELECT recording_id FROM file_raw_video UNION SELECT recording_id FROM file_transcoded_video) "
                    "ORDER BY date_recorded DESC")
        recordings = []
        for row in self.getConnection().execute(query):
            show = toAscii(row[1])
            episodeNumber = toEpisodeNumber(row[2])
            episode = toAscii(row[3])
            dateRecorded = toDatetime(row[4])
            duration = toDuration(row[5])
            recordings.append(Bunch(recordingID=row[0], show=show, episode=episode, episodeNumber=episodeNumber, dateRecorded=dateRecorded, duration=duration))
        return recordings

    def getRecentRecordings(self):
        # fetch from database
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded, recording.duration "
                    "FROM recording "
                    "INNER JOIN show ON (recording.show_id = show.show_id) "
                    "INNER JOIN episode ON (recording.show_id = episode.show_id AND recording.episode_id = episode.episode_id) "
                    "WHERE date_recorded > ? "
                    "ORDER BY date_recorded DESC")
        cutoffTime = (datetime.utcnow() - timedelta(days=2)).replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
        recordings = []
        for row in self.getConnection().execute(query, (cutoffTime,)):
            show = toAscii(row[1])
            episodeNumber = toEpisodeNumber(row[2])
            episode = toAscii(row[3])
            dateRecorded = toDatetime(row[4])
            duration = toDuration(row[5])
            recordings.append(Bunch(recordingID=row[0], show=show, episodeNumber=episodeNumber, episode=episode, dateRecorded=dateRecorded, duration=duration))
        return recordings

    def getUpcomingRecordings(self):
        query = str("SELECT schedule.start_time, schedule.channel_major, schedule.channel_minor,"
                    "show.show_id, show.name, episode.episode_id, episode.title "
                    "FROM schedule "
                    "INNER JOIN subscription ON (schedule.show_id = subscription.show_id) "
                    "INNER JOIN show ON (schedule.show_id = show.show_id) "
                    "INNER JOIN episode ON (schedule.show_id = episode.show_id AND schedule.episode_id = episode.episode_id) "
                    "WHERE schedule.start_time >= ? "
                    "AND schedule.show_id || '-' || schedule.episode_id NOT IN "
                        "(SELECT recorded_episodes_by_id.show_id || '-' || recorded_episodes_by_id.episode_id FROM recorded_episodes_by_id) "
                    "ORDER BY schedule.start_time, schedule.show_id, schedule.episode_id")
        currentTime = fromDatetime(datetime.utcnow())
        schedules = []
        for row in self.getConnection().execute(query, (currentTime,)):
            startTime = toDatetime(row[0])
            channel = '{}.{}'.format(row[1], row[2])
            showID = row[3]
            showName = toAscii(row[4])
            episodeNumber = toEpisodeNumber(row[5])
            episodeTitle = toAscii(row[6])
            schedules.append(Bunch(startTime=startTime, channel=channel, showID=showID, show=showName, episodeID=row[5], episodeNumber=episodeNumber, episode=episodeTitle))

        # sqlite select statements don't support "DISTINCT ON (column1, column2)", so we have to roll our own
        usedKeys = set()
        prunedSchedules = []
        for schedule in schedules:
            key = (schedule.showID, schedule.episodeID)
            if key not in usedKeys:
                prunedSchedules.append(schedule)
                usedKeys.add(key);
        return prunedSchedules

    def getShowList(self):
        subscribedShows = []
        for row in self.getConnection().execute('SELECT show.show_id, show.name FROM show, subscription WHERE show.show_id = subscription.show_id order by show.name'):
            showID = row[0]
            showName = toAscii(row[1])
            subscribedShows.append(Bunch(showID=showID, name=showName))
        unsubscribedShows = []
        for row in self.getConnection().execute('SELECT show_id, name FROM show WHERE show_id NOT IN (SELECT show_id FROM subscription) order by name'):
            showID = row[0]
            showName = toAscii(row[1])
            unsubscribedShows.append(Bunch(showID=showID, name=showName))
        return Bunch(subscribed=subscribedShows, unsubscribed=unsubscribedShows)

    def subscribe(self, showID):
        return self.insertSubscription(showID)

    def unsubscribe(self, showID):
        return self.deleteSubscription(showID)

    def getRecordingsWithoutFileRecords(self):
        query = str('SELECT recording.recording_id, show.name, episode.title, date_recorded '
                    'FROM recording '
                    'JOIN episode USING (show_id, episode_id) '
                    'JOIN show USING (show_id) '
                    'LEFT JOIN file_raw_video ON (recording.recording_id = file_raw_video.recording_id) '
                    'LEFT JOIN file_transcoded_video ON (recording.recording_id = file_transcoded_video.recording_id) '
                    'WHERE file_raw_video.filename IS NULL '
                    'AND file_transcoded_video.filename IS NULL')
        result = []
        for row in self.getConnection().execute(query):
            showName = toAscii(row[1])
            episodeName = toAscii(row[2])
            dateRecorded = toDatetime(row[3])
            result.append(Bunch(recordingID=row[0], show=showName, episode=episodeName, dateRecorded=dateRecorded))
        return result

#    def getFileRecordsWithoutRecordings(self):
#        query = str('SELECT recording_id, file_raw_video.filename, file_transcoded_video.filename, file_bif.filename '
#                    'FROM file_raw_video '
#                    'FULL JOIN file_transcoded_video USING (recording_id) '
#                    'FULL JOIN file_bif USING (recording_id) '
#                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording)'
#                    'ORDER BY recording_id')
#        result = []
#        for row in self.getConnection().execute(query):
#            result.append(Bunch(recordingID=row[0], rawVideo=row[1], transcodedVideo=row[2], bif=row[3]))
#        return result

    # sqlite does not support full joins, so we have to perform three separate queries and join them in python
    def getFileRecordsWithoutRecordings(self):
        query1 = str('SELECT recording_id, filename '
                     'FROM file_raw_video '
                     'WHERE recording_id NOT IN (SELECT recording_id FROM recording)')
        query2 = str('SELECT recording_id, filename '
                     'FROM file_transcoded_video '
                     'WHERE recording_id NOT IN (SELECT recording_id FROM recording)')
        query3 = str('SELECT recording_id, filename '
                     'FROM file_bif '
                     'WHERE recording_id NOT IN (SELECT recording_id FROM recording)')
        raw = { row[0]:row[1] for row in self.getConnection().execute(query1) }
        transcoded = { row[0]:row[1] for row in self.getConnection().execute(query2) }
        bif = { row[0]:row[1] for row in self.getConnection().execute(query3) }

        result = []
        for key in set(raw.keys()) | set(transcoded.keys()) | set(bif.keys()):
          result.append(Bunch(recordingID=key, rawVideo=raw.get(key), transcodedVideo=transcoded.get(key), bif=bif.get(key)))
        return result

    def getRawVideoFilesThatCanBeDeleted(self):
        query = str('SELECT recording_id, file_raw_video.filename, file_transcoded_video.filename '
                    'FROM file_raw_video '
                    'INNER JOIN file_transcoded_video USING (recording_id) '
                    'WHERE file_transcoded_video.state = 0 '
                    'ORDER BY file_raw_video.recording_id')
        result = []
        for row in self.getConnection().execute(query):
            result.append(Bunch(recordingID=row[0], rawVideo=row[1], transcodedVideo=row[2]))
        return result

    def getInconsistencies(self):
        return Bunch(
            recordingsWithoutFileRecords=self.getRecordingsWithoutFileRecords(),
            fileRecordsWithoutRecordings=self.getFileRecordsWithoutRecordings(),
            rawVideoFilesThatCanBeDeleted=self.getRawVideoFilesThatCanBeDeleted())

    def getTranscodingFailures(self):
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded "
                    "FROM recording "
                    'JOIN episode USING (show_id, episode_id) '
                    'JOIN show USING (show_id) '
                    "WHERE recording.recording_id IN (SELECT recording_id FROM file_transcoded_video WHERE state = 1) "
                    "ORDER BY date_recorded DESC")
        recordings = []
        for row in self.getConnection().execute(query):
            show = toAscii(row[1])
            episodeNumber = toEpisodeNumber(row[2])
            episodeTitle = toAscii(row[3])
            dateRecorded = toDatetime(row[4])
            recordings.append(Bunch(recordingID=row[0], show=show, episode=episodeTitle, episodeNumber=episodeNumber, dateRecorded=dateRecorded))
        return recordings

    def getPendingTranscodingJobs(self):
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded, recording.duration "
                    "FROM recording "
                    'JOIN episode USING (show_id, episode_id) '
                    'JOIN show USING (show_id) '
                    "WHERE recording.recording_id NOT IN (SELECT recording_id FROM file_transcoded_video) "
                    "AND recording.recording_id IN (SELECT recording_id FROM file_raw_video) "
                    "ORDER BY date_recorded DESC")
        recordings = []
        for row in self.getConnection().execute(query):
            showName = toAscii(row[1])
            episodeNumber = toEpisodeNumber(row[2])
            episodeTitle = toAscii(row[3])
            dateRecorded = toDatetime(row[4])
            duration = toDuration(row[5])
            recordings.append(Bunch(recordingID=row[0], show=showName, episode=episodeTitle, episodeNumber=episodeNumber, dateRecorded=dateRecorded, duration=duration))
        return recordings

    def insertTestShow(self):
        # is the 'test' show already present?
        cursor = self.getConnection().execute("SELECT show_id FROM show WHERE show_id = 'test'")
        if cursor.fetchone() is not None:
            return
        # insert the 'test' show
        self.getConnection().execute("INSERT INTO show (show_id, show_type, name, imageurl) VALUES ('test', 'EP', 'Test Show', NULL);")

    def scheduleTestRecording(self):
        self.insertTestShow()
        episodeID = self.getUniqueID()
        scheduleID = self.getUniqueID()
        query = str("INSERT INTO episode (show_id, episode_id, title, description, imageurl) "
                    "VALUES ('test', ?, 'TrinTV Test Episode', 'This is a test episode for TrinTV', NULL)")
        self.getConnection().execute(query, (episodeID, ))
        query = str("INSERT INTO schedule (schedule_id, channel_major, channel_minor, start_time, duration, show_id, episode_id, rerun_code) "
                    "VALUES (?, '41', '1', ?, 120, 'test', ?, 'R')")
        startTime = fromDatetime(datetime.utcnow() + timedelta(seconds=30))
        cursor = self.getConnection().execute(query, (scheduleID, startTime, episodeID))
        self.getConnection().commit()
        return cursor.rowcount

    def deleteFailedTranscode(self, recordingID):
        query = str("DELETE FROM file_transcoded_video WHERE recording_id = ? AND state = 1;")
        cursor = self.getConnection().execute(query, (recordingID, ))
        self.getConnection().commit()
        return cursor.rowcount


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SqliteDatabase utilities')
    parser.add_argument('dbFile', help='The filename of the database to open or initialize')
    parser.add_argument('--init', action='store_true',  help='Initialize database')
    args = parser.parse_args()
    if args.init:
        db = SqliteDatabase(args.dbFile)

