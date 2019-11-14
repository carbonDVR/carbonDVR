#!/usr/bin/env python3

import sqlite3
from datetime import datetime, timedelta
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

    def close(self):
        self.connection.close()

    def commit(self):
        self.connection.commit()

    def initializeSchema(self):
        schemaScript = '''
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
            duration       interval,
            rerun_code     text,
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
        self.connection.execute("INSERT INTO uniqueid VALUES (1)")
        self.connection.commit()

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
        numRowsInserted = 0
        cursor = self.connection.execute(
            'INSERT INTO schedule(channel_major, channel_minor, start_time, duration, show_id, episode_id, rerun_code) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (channelMajor, channelMinor, startTime.isoformat(), str(duration), showID, episodeID, rerunCode))
        self.connection.commit()
        return cursor.rowcount

    def insertRecording(self, recordingID, showID, episodeID, dateRecorded, duration, rerunCode):
        cursor = self.connection.execute(
            "INSERT INTO recording(recording_id, show_id, episode_id, date_recorded, duration, rerun_code) VALUES (?, ?, ?, ?, ?, ?)",
            (recordingID, showID, episodeID, dateRecorded, str(duration), rerunCode))
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
            schedules.append(Bunch(channelMajor=row[1], channelMinor=row[2], startTime=row[3], duration=row[4], showID=row[5], episodeID=row[6], rerunCode=row[7]))
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
        startTime = datetime.utcnow().isoformat()
        endTime = (datetime.utcnow() + timedelta(hours=12)).isoformat()
        schedules = []
        for row in self.connection.execute(query, (startTime, endTime)):
            startTime = datetime.strptime(row[3], "%Y-%m-%dT%H:%M:%S.%f")
            schedules.append(Bunch(channelMajor=row[1], channelMinor=row[2], startTime=startTime, duration=row[4], showID=row[5], episodeID=row[6], rerunCode=row[7]))

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
                    'ORDER BY recording_id;')
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
                    'ORDER BY file_raw_video.recording_id;')
        records = []
        for row in self.connection.execute(query):
            records.append(Bunch(recordingID=row[0], filename=row[1]))
        return records

