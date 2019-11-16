#!/usr/bin/env python3

import tzlocal
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from enum import Enum



class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class TranscodingState(Enum):
    SUCCESS = 0
    FAILURE = 1


class SqliteDatabase:
    TRANSCODE_SUCCESSFUL = 0
    TRANSCODE_FAILED = 1

    def __init__(self, dbFile):
        self.connection = sqlite3.connect(dbFile, isolation_level=None)
#        self.connection.autocommit = True
        self.initializeSchema()

    def close(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()

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
        self.connection.executescript(schemaScript)
        self.connection.execute("INSERT INTO schema_version VALUES (1)")
        self.connection.execute("INSERT INTO uniqueid VALUES (1)")
        self.connection.commit()

    def getSchemaVersion(self):
      try:
        cursor = self.connection.execute("SELECT version FROM schema_version");
        if cursor.rowcount == 0:
          raise Exception('[43YWBN] no rows in schema table.'.format(cursor.rowcount))
        if cursor.rowcount > 1:
          raise Exception('[43YWBP] too many rows in schema table.  rowcount={}'.format(cursor.rowcount))
        return cursor.fetchone()[0]
      except sqlite3.OperationalError:
        return 0

    def insertShow(self, showID, showType, name):
        cursor = self.connection.execute(
            "INSERT INTO show(show_id, show_type, name) VALUES (?, ?, ?)",
            (showID, showType, name))
        self.connection.commit()
        return cursor.rowcount

    def insertSubscription(self, showID, priority):
        cursor = self.connection.execute(
            "INSERT INTO subscription(show_id, priority) VALUES (?, ?)",
            (showID, priority))
        self.connection.commit()
        return cursor.rowcount

    def deleteSubscription(self, showID):
        cursor = self.connection.execute('DELETE FROM subscription WHERE show_id = ?', (showID, ))
        self.connection.commit()
        return cursor.rowcount

    def insertEpisode(self, showID, episodeID, title, description):
        cursor = self.connection.execute(
            "INSERT INTO episode(show_id, episode_id, title, description) VALUES (?, ?, ?, ?)",
            (showID, episodeID, title, description))
        self.connection.commit()
        return cursor.rowcount

    def insertSchedule(self, channelMajor, channelMinor, startTime, duration, showID, episodeID, rerunCode):
        if startTime.tzinfo is None:
          startTime = startTime.replace(tzinfo=timezone.utc)
        startTimeString = startTime.strftime("%Y-%m-%dT%H:%M:%S%z")
        numRowsInserted = 0
        cursor = self.connection.execute(
            'INSERT INTO schedule(channel_major, channel_minor, start_time, duration, show_id, episode_id, rerun_code) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (channelMajor, channelMinor, startTimeString, duration.total_seconds(), showID, episodeID, rerunCode))
        self.connection.commit()
        return cursor.rowcount

    def insertRecording(self, recordingID, showID, episodeID, dateRecorded, duration, categoryCode):
        dateRecordedString = dateRecorded.strftime("%Y-%m-%dT%H:%M:%S%z")
        cursor = self.connection.execute(
            "INSERT INTO recording(recording_id, show_id, episode_id, date_recorded, duration, category_code) VALUES (?, ?, ?, ?, ?, ?)",
            (recordingID, showID, episodeID, dateRecordedString, duration.total_seconds(), categoryCode))
        self.connection.commit()
        return cursor.rowcount

    def insertTuner(self, deviceID, ipaddress, tunerID):
        cursor = self.connection.execute(
            "INSERT INTO tuner(device_id, ipaddress, tuner_id) VALUES (?, ?, ?)",
            (deviceID, ipaddress, tunerID))
        self.connection.commit()
        return cursor.rowcount

    def insertChannel(self, channelMajor, channelMinor, channelActual, program):
        cursor = self.connection.execute(
            "INSERT INTO channel(major, minor, actual, program) VALUES (?, ?, ?, ?)",
            (channelMajor, channelMinor, channelActual, program))
        self.connection.commit()
        return cursor.rowcount

    def getChannels(self):
        channels = []
        for row in self.connection.execute("SELECT major, minor, actual, program FROM channel"):
          channels.append(Bunch(channelMajor=row[0], channelMinor=row[1], channelActual=row[2], program=row[3]))
        return channels

    def getTuners(self):
        tuners = []
        for row in self.connection.execute("SELECT device_id, ipaddress, tuner_id FROM tuner"):
          tuners.append(Bunch(deviceID=row[0], ipAddress=row[1], tunerID=row[2]))
        return tuners

    # this is the correct version, which we can't use until we install sqlite version 3.15.0 or higher
    def getPendingRecordings_correct(self):
#        query = str("SELECT DISTINCT ON (schedule.show_id, schedule.episode_id) "
        query = str("SELECT "
                    "schedule.schedule_id, schedule.channel_major, schedule.channel_minor, schedule.start_time, "
                    "schedule.duration, schedule.show_id, schedule.episode_id, schedule.rerun_code "
                    "FROM schedule "
                    "INNER JOIN subscription ON (schedule.show_id = subscription.show_id) "
                    "WHERE schedule.start_time > datetime('now') "
                    "AND schedule.start_time < datetime('now', '+12 hours')"
                    "AND (schedule.show_id, schedule.episode_id) NOT IN "
                        "(SELECT recorded_episodes_by_id.show_id, recorded_episodes_by_id.episode_id FROM recorded_episodes_by_id) "
                    "ORDER BY schedule.show_id, schedule.episode_id, schedule.start_time;")
        schedules = []
        for row in self.connection.execute(query):
            duration = timedelta(seconds=row[4])
            schedules.append(Bunch(channelMajor=row[1], channelMinor=row[2], startTime=row[3], duration=duration, showID=row[5], episodeID=row[6], rerunCode=row[7]))
        return schedules

    # this is the workaround hack
    def getPendingRecordings(self):
#        query = str("SELECT DISTINCT ON (schedule.show_id, schedule.episode_id) "
        query = str("SELECT "
                    "schedule.schedule_id, schedule.channel_major, schedule.channel_minor, schedule.start_time, "
                    "schedule.duration, schedule.show_id, schedule.episode_id, schedule.rerun_code "
                    "FROM schedule "
                    "INNER JOIN subscription ON (schedule.show_id = subscription.show_id) "
                    "WHERE schedule.start_time > ? "
                    "AND schedule.start_time < ? "
                    "AND schedule.show_id || '-' || schedule.episode_id NOT IN "
                        "(SELECT recorded_episodes_by_id.show_id || '-' || recorded_episodes_by_id.episode_id FROM recorded_episodes_by_id) "
                    "ORDER BY schedule.show_id, schedule.episode_id, schedule.start_time;")
        startTime = datetime.utcnow().replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
        endTime = (datetime.utcnow() + timedelta(hours=12)).replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
        schedules = []
        for row in self.connection.execute(query, (startTime, endTime)):
            startTime = datetime.strptime(row[3], "%Y-%m-%dT%H:%M:%S%z")
            duration = timedelta(seconds=row[4])
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
        self.connection.execute("BEGIN TRANSACTION")
        uniqueID = self.connection.execute("SELECT nextID FROM uniqueid").fetchone()[0]
        self.connection.execute("DELETE FROM uniqueid")
        self.connection.execute("INSERT INTO uniqueid VALUES (?)", (uniqueID + 1,))
        self.connection.execute("COMMIT")
        return uniqueID

    def insertRawFileLocation(self, recordingID, filename):
        cursor = self.connection.execute("INSERT INTO file_raw_video(recording_id, filename) VALUES (?, ?);", (recordingID, filename))
        self.connection.commit()
        return cursor.rowcount

    def insertTranscodedFileLocation(self, recordingID, locationID, filename, state):
        cursor = self.connection.execute(
            "INSERT INTO file_transcoded_video(recording_id, location_id, filename, state) VALUES (?, ?, ?, ?)",
            (recordingID, locationID, filename, state))
        self.connection.commit()
        return cursor.rowcount

    def getRecordingsToBif(self):
        cursor = self.connection.execute(
            "SELECT recording_id, filename FROM file_transcoded_video WHERE state = 0 AND recording_id NOT IN (SELECT recording_id FROM file_bif);")
        recordings = []
        for row in cursor:
            recordings.append(Bunch(recordingID=row[0], filename=row[1]))
        self.connection.commit()
        return recordings

    def insertBifFileLocation(self, recordingID, locationID, filename):
        cursor = self.connection.execute(
            "INSERT INTO file_bif(recording_id, location_id, filename) VALUES (?, ?, ?)",
            (recordingID, locationID, filename))
        self.connection.commit()
        return cursor.rowcount

    def getUnreferencedRawVideoRecords(self):
        query = str('SELECT recording_id, filename '
                    'FROM file_raw_video '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id;')
        records = []
        for row in self.connection.execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def deleteRawVideoRecord(self, recordingID):
        cursor = self.connection.execute('DELETE FROM file_raw_video WHERE recording_id = ?', (recordingID, ))
        self.connection.commit()
        return cursor.rowcount

    def getUnreferencedTranscodedVideoRecords(self):
        query = str('SELECT recording_id, filename '
                    'FROM file_transcoded_video '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id;')
        records = []
        for row in self.connection.execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def deleteTranscodedVideoRecord(self, recordingID):
        cursor = self.connection.execute('DELETE FROM file_transcoded_video WHERE recording_id = ?', (recordingID, ))
        self.connection.commit()
        return cursor.rowcount

    def getUnreferencedBifRecords(self):
        query = str('SELECT recording_id, filename '
                    'FROM file_bif '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording) '
                    'ORDER BY recording_id')
        records = []
        for row in self.connection.execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def deleteBifRecord(self, recordingID):
        cursor = self.connection.execute('DELETE FROM file_bif WHERE recording_id = ?', (recordingID, ))
        self.connection.commit()
        return cursor.rowcount

    def getUnneededRawVideoRecords(self):
        query = str('SELECT file_raw_video.recording_id, file_raw_video.filename '
                    'FROM file_raw_video '
                    'INNER JOIN file_transcoded_video USING (recording_id) '
                    'WHERE file_transcoded_video.state = 0 '
                    'ORDER BY file_raw_video.recording_id')
        records = []
        for row in self.connection.execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

    def selectRecordingsToTranscode(self):
        query = "SELECT recording_id, filename FROM file_raw_video WHERE recording_id NOT IN (SELECT recording_id FROM file_transcoded_video)"
        recordings = []
        for row in self.connection.execute(query):
           recordings.append(Bunch(recordingID=row[0], filename=row[1]))
        return recordings

    def getDuration(self, recordingID):
        cursor = self.connection.execute("SELECT duration FROM recording WHERE recording_id = ?", (recordingID,))
        row = cursor.fetchone()
        if row :
            return timedelta(seconds=row[0])
        return timedelta(seconds=0)

    def getShowsWithRecordings(self, categoryCodes):
        query = str("SELECT DISTINCT recording.show_id, show.name, show.imageURL "
                    "FROM recording, show "
                    "WHERE recording.show_id = show.show_id "
                    "AND recording.recording_id IN (SELECT recording_id FROM file_bif) "
                    "AND recording.category_code = ?"
                    "ORDER BY show.name")
        shows = []
        for categoryCode in categoryCodes:
          for row in self.connection.execute(query, (categoryCode,)):
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
            for row in self.connection.execute(query, (showID, categoryCode)):
                episodeNumber = int(re.match('\d*', row[2]).group(0)) # match only leading digits (e.g. '1_2' is converted to '1')
                episodeTitle = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeDescription = row[4].encode('ascii', 'xmlcharrefreplace').decode('ascii')     # compensate for Python's inability to cope with unicode
                recordings.append({
                    'recordingID':row[0],
                    'showID':row[1],
                    'episodeID':row[2],
                    'episodeTitle':episodeTitle,
                    'episodeDescription':episodeDescription,
                    'imageURL':row[5],
                    'showImageURL':row[6],
                    'episodeNumber':episodeNumber})
        recordings = sorted(recordings, key=lambda x:x['episodeNumber'])
        return recordings

    def getRecordingData(self, recordingID):
        query = str("SELECT recording.recording_id, show.name, show.imageurl, episode.title, episode.description, episode.episode_id, "
                    "recording.date_recorded, recording.duration "
                    "FROM recording, show, episode "
                    "WHERE recording.show_id = show.show_id "
                    "AND recording.show_id = episode.show_id "
                    "AND recording.episode_id = episode.episode_id "
                    "AND recording.recording_id = ?")
        cursor = self.connection.execute(query, (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return None
        showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')               # compensate for Python's inability to cope with unicode
        showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')               # compensate for Python's inability to cope with unicode
        episodeTitle = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
        episodeDescription = row[4].encode('ascii', 'xmlcharrefreplace').decode('ascii')     # compensate for Python's inability to cope with unicode
        episodeNumber = re.match('\d*', row[5]).group(0) # match only leading digits (e.g. '1_2' is converted to '1')
        dateRecorded = datetime.strptime(row[6], "%Y-%m-%dT%H:%M:%S%z").astimezone(tzlocal.get_localzone())
        duration = timedelta(seconds=row[7])
        recordingData = {'recordingID':row[0], 'showName':showName, 'imageURL':row[2], 'episodeTitle':episodeTitle, 'episodeDescription':episodeDescription, 'dateRecorded':dateRecorded, 'duration':duration, 'episodeNumber':episodeNumber}
        return recordingData

    def getTranscodedVideoLocationID(self, recordingID):
        cursor = self.connection.execute('SELECT location_id FROM file_transcoded_video WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return 0
        return row[0]

    def getBifLocationID(self, recordingID):
        cursor = self.connection.execute('SELECT location_id FROM file_bif WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
            return 0
        return row[0]

    def deleteRecording(self, recordingID):
        cursor = self.connection.execute('DELETE FROM recording WHERE recording_id = ?', (recordingID, ))
        self.connection.commit()
        return cursor.rowcount

    def setPlaybackPosition(self, recordingID, playbackPosition):
        cursor = self.connection.execute('UPDATE playback_position SET position = ? WHERE recording_id = ?', (playbackPosition, recordingID))
        if cursor.rowcount == 0:
            cursor = self.connection.execute('INSERT INTO playback_position (recording_id, position) VALUES (?, ?)', (recordingID, playbackPosition))
        self.connection.commit()
        return cursor.rowcount

    def getPlaybackPosition(self, recordingID):
        cursor = self.connection.execute('SELECT position FROM playback_position WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return {'playbackPosition': 0}
        return {'playbackPosition': row[0]}

    def setCategoryCode(self, recordingID, categoryCode):
        cursor = self.connection.execute('UPDATE recording SET category_code = ? WHERE recording_id = ?', (categoryCode, recordingID))
        self.connection.commit()
        return cursor.rowcount

    def getCategoryCode(self, recordingID):
        cursor = self.connection.execute('SELECT category_code FROM recording WHERE recording_id = ?', (recordingID, ))
        row = cursor.fetchone()
        if not row:
          return ''
        return row[0]

    def getRemainingListingTime(self):
        cursor = self.connection.execute('''SELECT max(start_time) FROM schedule;''')
        row = cursor.fetchone()
        if not row:
          return timedelta(seconds=0)
        latestListing = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%S%z")
        return latestListing - datetime.utcnow().replace(tzinfo=timezone.utc)

