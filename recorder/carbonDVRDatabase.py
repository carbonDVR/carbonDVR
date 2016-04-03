#!/usr/bin/env python3

import psycopg2
from datetime import datetime


# trivial class to allow easy construction of records
# Example: foo = Bunch(a=1, b=5, c='moose')
#          print(foo.c)
class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class ChannelInfo:
    def __init__(self, channelMajor, channelMinor, channelActual, program):
        self.channelMajor = channelMajor
        self.channelMinor = channelMinor
        self.channelActual = channelActual
        self.program = program


class TunerInfo:
    def __init__(self, deviceID, ipAddress, tunerID):
        self.deviceID = deviceID
        self.ipAddress = ipAddress
        self.tunerID = int(tunerID)


class CarbonDVRDatabase:
    def __init__(self, dbConnection):
        self.connection = dbConnection
        self.connection.autocommit = True

    def getChannels(self):
        channels = []
        with self.connection.cursor() as cursor:
          cursor.execute("SELECT major, minor, actual, program FROM channel")
          for row in cursor:
            channels.append(ChannelInfo(channelMajor=row[0], channelMinor=row[1], channelActual=row[2], program=row[3]))
        self.connection.commit()
        return channels

    def getTuners(self):
        tuners = []
        with self.connection.cursor() as cursor:
          cursor.execute("SELECT device_id, ipaddress, tuner_id FROM tuner")
          for row in cursor:
            tuners.append(TunerInfo(deviceID=row[0], ipAddress=row[1], tunerID=row[2]))
        self.connection.commit()
        return tuners

    def getPendingRecordings(self, lookaheadTime):
        schedules = []
        with self.connection.cursor() as cursor:
            query = str("SELECT DISTINCT ON (schedule.show_id, schedule.episode_id) "
                        "schedule.schedule_id, schedule.channel_major, schedule.channel_minor, schedule.start_time, "
                        "schedule.duration, schedule.show_id, schedule.episode_id, schedule.rerun_code "
                        "FROM schedule "
                        "INNER JOIN subscription ON (schedule.show_id = subscription.show_id) "
                        "WHERE schedule.start_time > now() "
                        "AND schedule.start_time < now() + %s "
                        "AND (schedule.show_id, schedule.episode_id) NOT IN "
                            "(SELECT recorded_episodes_by_id.show_id, recorded_episodes_by_id.episode_id FROM recorded_episodes_by_id) "
                        "ORDER BY schedule.show_id, schedule.episode_id;");
            cursor.execute(query, (lookaheadTime, ))
            for row in cursor:
                schedules.append(Bunch(channelMajor=row[1], channelMinor=row[2], startTime=row[3], duration=row[4], showID=row[5], episodeID=row[6], rerunCode=row[7]))
        self.connection.commit()
        return schedules

    def getUniqueID(self):
        uniqueID = None
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT nextval('uniqueid');", ())
            if cursor:
                uniqueID = cursor.fetchone()[0]
        self.connection.commit()
        return uniqueID

    def insertRecording(self, recordingID, showID, episodeID, duration, rerunCode):
        rowCount = 0
        with self.connection.cursor() as cursor:
            query = str("INSERT INTO recording(recording_id, show_id, episode_id, date_recorded, duration, rerun_code) "
                        "VALUES (%s, %s, %s, now(), %s, %s);")
            cursor.execute(query, (recordingID, showID, episodeID, duration, rerunCode))
            rowCount = cursor.rowcount
        self.connection.commit()
        return rowCount

    def insertRawVideoLocation(self, recordingID, filename):
        rowCount = 0
        with self.connection.cursor() as cursor:
            cursor.execute("INSERT INTO file_raw_video(recording_id, filename) VALUES (%s, %s);", (recordingID, filename))
            rowCount = cursor.rowcount
        self.connection.commit()
        return rowCount

