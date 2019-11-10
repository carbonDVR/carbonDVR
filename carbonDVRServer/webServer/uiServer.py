#!/usr/bin/env python3.4

from flask import render_template
import logging
import os
import psycopg2
import tzlocal

from psycopg2.extensions import register_type, UNICODE
register_type(UNICODE)


class Bunch():
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class UIServer:
    def __init__(self, dbConnection, uiServerURL, scheduleRecordingsCallback):
        self.dbConnection = dbConnection
        self.uiServerURL = uiServerURL
        self.scheduleRecordingsCallback = scheduleRecordingsCallback

    def makeURL(self, endpoint):
        return self.uiServerURL + endpoint

    def dbGetAllRecordings(self):
        recordings = []
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded, recording.duration "
                    "FROM recording "
                    "INNER JOIN show ON (recording.show_id = show.show_id) "
                    "INNER JOIN episode ON (recording.show_id = episode.show_id AND recording.episode_id = episode.episode_id) "
                    "WHERE recording.recording_id IN (SELECT recording_id FROM file_raw_video UNION SELECT recording_id FROM file_transcoded_video) "
                    "ORDER BY date_recorded DESC;")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                show = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeNumber = row[2].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                episode = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')        # compensate for Python's inability to cope with unicode
                dateRecorded = row[4].astimezone(tzlocal.get_localzone())
                recordings.append(Bunch(recordingID=row[0], show=show, episode=episode, episodeNumber=episodeNumber, dateRecorded=dateRecorded, duration=row[5]))
        self.dbConnection.commit()
        return recordings


    def dbGetRecentRecordings(self):
        # fetch from database
        recordings = []
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded, recording.duration "
                    "FROM recording "
                    "INNER JOIN show ON (recording.show_id = show.show_id) "
                    "INNER JOIN episode ON (recording.show_id = episode.show_id AND recording.episode_id = episode.episode_id) "
                    "WHERE date_recorded > now() - interval '2 days' "
                    "ORDER BY date_recorded DESC;")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                show = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeNumber = row[2].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                episode = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')        # compensate for Python's inability to cope with unicode
                dateRecorded = row[4].astimezone(tzlocal.get_localzone())
                recordings.append(Bunch(recordingID=row[0], show=show, episodeNumber=episodeNumber, episode=episode, dateRecorded=dateRecorded, duration=row[5]))
        self.dbConnection.commit()
        return recordings


    def dbGetUpcomingRecordings(self):
        schedules = []
        query = str("SELECT DISTINCT ON (schedule.show_id, schedule.episode_id) "
                    "schedule.schedule_id, schedule.start_time, schedule.channel_major, schedule.channel_minor,"
                    "show.name, episode.episode_id, episode.title "
                    "FROM schedule "
                    "INNER JOIN subscription ON (schedule.show_id = subscription.show_id) "
                    "INNER JOIN show ON (schedule.show_id = show.show_id) "
                    "INNER JOIN episode ON (schedule.show_id = episode.show_id AND schedule.episode_id = episode.episode_id) "
                    "WHERE schedule.start_time > now() "
                    "AND (schedule.show_id, schedule.episode_id) NOT IN "
                        "(SELECT recorded_episodes_by_id.show_id, recorded_episodes_by_id.episode_id FROM recorded_episodes_by_id) "
                    "ORDER BY schedule.show_id, schedule.episode_id ")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                startTime = row[1].astimezone(tzlocal.get_localzone())
                channel = '{}.{}'.format(row[2], row[3])
                show = row[4].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeNumber = row[5].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                episode = row[6].encode('ascii', 'xmlcharrefreplace').decode('ascii')        # compensate for Python's inability to cope with unicode
                schedules.append(Bunch(scheduleID=row[0], startTime=startTime, channel=channel, show=show, episodeNumber=episodeNumber, episode=episode))
        self.dbConnection.commit()
        schedules.sort(key=lambda schedule: schedule.startTime)
        return schedules


    def dbGetShowList(self):
        subscribedShows = []
        unsubscribedShows = []
        with self.dbConnection.cursor() as cursor:
            cursor.execute('SELECT show.show_id, show.name FROM show, subscription WHERE show.show_id = subscription.show_id order by show.name;')
            for row in cursor:
                showID = row[0]
                showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                subscribedShows.append(Bunch(showID=showID, name=showName))
            cursor.execute('SELECT show_id, name FROM show WHERE show_id NOT IN (SELECT show_id FROM subscription) order by name;')
            for row in cursor:
                showID = row[0]
                showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                unsubscribedShows.append(Bunch(showID=showID, name=showName))
        self.dbConnection.commit()
        return Bunch(subscribed=subscribedShows, unsubscribed=unsubscribedShows)


    def dbSubscribe(self, showID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('INSERT INTO subscription (show_id, priority) VALUES (%s, %s);', (showID, 0 ))
        self.dbConnection.commit()


    def dbUnsubscribe(self, showID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute('DELETE FROM subscription WHERE show_id = %s;', (showID, ))
        self.dbConnection.commit()


    def dbGetInconsistencies(self):
        recordingsWithoutFileRecords = []
        fileRecordsWithoutRecordings = []
        rawVideoFilesThatCanBeDeleted= []
        query = str('SELECT recording.recording_id, show.name, episode.title, date_recorded '
                    'FROM recording '
                    'JOIN show USING (show_id) '
                    'JOIN episode USING (show_id, episode_id) '
                    'LEFT JOIN file_raw_video ON (recording.recording_id = file_raw_video.recording_id) '
                    'LEFT JOIN file_transcoded_video ON (recording.recording_id = file_transcoded_video.recording_id) '
                    'WHERE file_raw_video.filename IS NULL '
                    'AND file_transcoded_video.filename IS NULL;')
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                showName = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')     # compensate for Python's inability to cope with unicode
                episodeName = row[2].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                dateRecorded = row[3].astimezone(tzlocal.get_localzone())
                recordingsWithoutFileRecords.append(Bunch(recordingID=row[0], show=showName, episode=episodeName, dateRecorded=dateRecorded))
        query = str('SELECT recording_id, file_raw_video.filename, file_transcoded_video.filename, file_bif.filename '
                    'FROM file_raw_video '
                    'FULL JOIN file_transcoded_video USING (recording_id) '
                    'FULL JOIN file_bif USING (recording_id) '
                    'WHERE recording_id NOT IN (SELECT recording_id FROM recording);')
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                fileRecordsWithoutRecordings.append(Bunch(recordingID=row[0], rawVideo=row[1], transcodedVideo=row[2], bif=row[3]))
        query = str('SELECT recording_id, file_raw_video.filename, file_transcoded_video.filename '
                        'FROM file_raw_video '
                        'INNER JOIN file_transcoded_video USING (recording_id) '
                        'WHERE file_transcoded_video.state = 0 '
                        'ORDER BY file_raw_video.recording_id;')
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                rawVideoFilesThatCanBeDeleted.append(Bunch(recordingID=row[0], rawVideo=row[1], transcodedVideo=row[2]))
        self.dbConnection.commit()
        return Bunch(recordingsWithoutFileRecords=recordingsWithoutFileRecords, fileRecordsWithoutRecordings=fileRecordsWithoutRecordings, rawVideoFilesThatCanBeDeleted=rawVideoFilesThatCanBeDeleted)


    def dbGetTranscodingFailures(self):
        recordings = []
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded "
                    "FROM recording "
                    'JOIN show USING (show_id) '
                    'JOIN episode USING (show_id, episode_id) '
                    "WHERE recording.recording_id IN (SELECT recording_id FROM file_transcoded_video WHERE state = 1) "
                    "ORDER BY date_recorded DESC;")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                show = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeNumber = row[2].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                episode = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')        # compensate for Python's inability to cope with unicode
                dateRecorded = row[4].astimezone(tzlocal.get_localzone())
                recordings.append(Bunch(recordingID=row[0], show=show, episode=episode, episodeNumber=episodeNumber, dateRecorded=dateRecorded))
        self.dbConnection.commit()
        return recordings


    def dbGetPendingTranscodingJobs(self):
        recordings = []
        query = str("SELECT recording.recording_id, show.name, episode.episode_id, episode.title, recording.date_recorded, recording.duration "
                    "FROM recording "
                    'JOIN show USING (show_id) '
                    'JOIN episode USING (show_id, episode_id) '
                    "WHERE recording.recording_id NOT IN (SELECT recording_id FROM file_transcoded_video) "
                    "AND recording.recording_id IN (SELECT recording_id FROM file_raw_video) "
                    "ORDER BY date_recorded DESC;")
        with self.dbConnection.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                show = row[1].encode('ascii', 'xmlcharrefreplace').decode('ascii')           # compensate for Python's inability to cope with unicode
                episodeNumber = row[2].encode('ascii', 'xmlcharrefreplace').decode('ascii')  # compensate for Python's inability to cope with unicode
                episode = row[3].encode('ascii', 'xmlcharrefreplace').decode('ascii')        # compensate for Python's inability to cope with unicode
                dateRecorded = row[4].astimezone(tzlocal.get_localzone())
                recordings.append(Bunch(recordingID=row[0], show=show, episode=episode, episodeNumber=episodeNumber, dateRecorded=dateRecorded, duration=row[5]))
        self.dbConnection.commit()
        return recordings


    def dbGetNextScheduleID(self):
        scheduleID = None
        with self.dbConnection.cursor() as cursor:
            cursor.execute("SELECT nextval('schedule_schedule_id_seq');", ())
            row = cursor.fetchone()
            if row:
                scheduleID = row[0]
        self.dbConnection.commit()
        return scheduleID


    def dbInsertTestShow(self):
        with self.dbConnection.cursor() as cursor:
            # is the 'test' show already present?
            cursor.execute("SELECT show_id FROM show WHERE show_id = 'test';")
            if cursor.fetchone() is not None:
                return
            # insert the 'test' show
            cursor.execute("INSERT INTO show (show_id, show_type, name, imageurl) VALUES ('test','EP','Test Show',NULL);")


    def dbScheduleTestRecording(self):
        self.dbInsertTestShow()
        uniqueID = self.dbGetNextScheduleID()
        with self.dbConnection.cursor() as cursor:
            query = str("INSERT INTO episode (show_id, episode_id, title, description, imageurl) "
                        "VALUES ('test', %s, 'TrinTV Test Episode', 'This is a test episode for TrinTV', NULL);")
            cursor.execute(query, (uniqueID, ))
            query = str("INSERT INTO schedule (schedule_id, channel_major, channel_minor, start_time, duration, show_id, episode_id, rerun_code) "
                        "VALUES (%s, '41', '1', now() + '30 seconds', '2 minutes', 'test', %s, 'R');")
            cursor.execute(query, (uniqueID, uniqueID))
        self.dbConnection.commit()


    def dbDeleteFailedTranscode(self, recordingID):
        with self.dbConnection.cursor() as cursor:
            query = str("DELETE FROM file_transcoded_video WHERE recording_id = %s AND state = 1;")
            cursor.execute(query, (recordingID, ))
        self.dbConnection.commit()


    def getIndex(self):
        return render_template('index.html')


    def getRecordingsByDate(self):
        recordings = self.dbGetAllRecordings()
        return render_template('recordingsByDate.html', recordings=recordings)


    def getRecentRecordings(self):
        recordings = self.dbGetRecentRecordings()
        return render_template('recentRecordings.html', recordings=recordings)


    def getUpcomingRecordings(self):
        schedules = self.dbGetUpcomingRecordings()
        return render_template('upcomingRecordings.html', schedules=schedules)


    def getShowList(self):
        shows = self.dbGetShowList()
        return render_template('showList.html', subscribedShows=shows.subscribed, unsubscribedShows=shows.unsubscribed)


    def subscribe(self, showID):
        self.dbSubscribe(showID)


    def unsubscribe(self, showID):
        self.dbUnsubscribe(showID)


    def getDatabaseInconsistencies(self):
        inconsistencies = self.dbGetInconsistencies()
        return render_template('databaseInconsistencies.html', recordingsWithoutFileRecords=inconsistencies.recordingsWithoutFileRecords,
            fileRecordsWithoutRecordings=inconsistencies.fileRecordsWithoutRecordings, rawVideoFilesThatCanBeDeleted=inconsistencies.rawVideoFilesThatCanBeDeleted)


    def scheduleTestRecording(self):
        self.dbScheduleTestRecording()
        self.scheduleRecordingsCallback()


    def getRecordingsByShow(self):
        allRecordings = self.dbGetAllRecordings()
        showList = []
        recordingsByShow = {}
        for showName in set([x.show for x in allRecordings]):
          recordings = [x for x in allRecordings if x.show == showName]
          showData = Bunch(name=showName, numRecordings=len(recordings))
          showList.append(showData)
          recordingsByShow[showData] = recordings
        showList = sorted(showList, key=lambda x: x.name)
        return render_template('recordingsByShow.html', showList=showList, recordingsByShow=recordingsByShow)

    def getTranscodingFailures(self):
        transcodingFailures = self.dbGetTranscodingFailures()
        return render_template('transcodingFailures.html', recordings=transcodingFailures)

    def getPendingTranscodingJobs(self):
        pendingTranscodingJobs = self.dbGetPendingTranscodingJobs()
        return render_template('pendingTranscodingJobs.html', recordings=pendingTranscodingJobs)

    def retryTranscode(self, recordingID):
        self.dbDeleteFailedTranscode(recordingID)
