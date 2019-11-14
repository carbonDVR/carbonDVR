import os
import sqlite3
import unittest
from sqliteDatabase import SqliteDatabase, TranscodingState
from datetime import datetime, timedelta


class TestSqliteDatabase(unittest.TestCase):
    def test_getTuners(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertTuner('foo','192.168.1.1',1))
        self.assertEqual(1, db.insertTuner('baz','10.10.10.1',0))
        tuners = sorted(db.getTuners(), key=lambda tuner:tuner.deviceID)
        self.assertEqual(2, len(tuners))
        self.assertEqual('baz', tuners[0].deviceID)
        self.assertEqual('10.10.10.1', tuners[0].ipAddress)
        self.assertEqual(0, tuners[0].tunerID)
        self.assertEqual('foo', tuners[1].deviceID)
        self.assertEqual('192.168.1.1', tuners[1].ipAddress)
        self.assertEqual(1, tuners[1].tunerID)
        
    def test_getChannels(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertChannel(4,1,5,1))
        self.assertEqual(1, db.insertChannel(19,3,18,2))
        self.assertEqual(1, db.insertChannel(5,2,37,3))
        channels = sorted(db.getChannels(), key=lambda channel:channel.channelMajor)
        self.assertEqual(3, len(channels))
        self.assertEqual(4, channels[0].channelMajor)
        self.assertEqual(1, channels[0].channelMinor)
        self.assertEqual(5, channels[0].channelActual)
        self.assertEqual(1, channels[0].program)
        self.assertEqual(5, channels[1].channelMajor)
        self.assertEqual(2, channels[1].channelMinor)
        self.assertEqual(37, channels[1].channelActual)
        self.assertEqual(3, channels[1].program)
        self.assertEqual(19, channels[2].channelMajor)
        self.assertEqual(3, channels[2].channelMinor)
        self.assertEqual(18, channels[2].channelActual)
        self.assertEqual(2, channels[2].program)

    def test_getUniqueID(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        uniqueID1 = db.getUniqueID();
        uniqueID2 = db.getUniqueID();
        uniqueID3 = db.getUniqueID();
        uniqueID4 = db.getUniqueID();
        self.assertEqual(uniqueID1+1, uniqueID2)
        self.assertEqual(uniqueID1+2, uniqueID3)
        self.assertEqual(uniqueID1+3, uniqueID4)

    def test_getPendingRecordings(self):
        db = SqliteDatabase(":memory:")
#        db = SqliteDatabase("/tmp/jnm001.dat")
        db.initializeSchema()
        db.insertChannel(channelMajor=1, channelMinor=1, channelActual=14, program=1)
        db.insertShow(showID="show1", showType="comedy", name="Show #1")
        db.insertSubscription(showID="show1", priority=1)
        db.insertEpisode(showID="show1", episodeID="episode1", title="Show 1, Episode 1", description="")
        db.insertEpisode(showID="show1", episodeID="episode2", title="Show 1, Episode 2", description="")

        # scheduled recordings should be returned
        scheduleTime1 = datetime.utcnow() + timedelta(hours=1)
        scheduleDuration1 = timedelta(minutes=30)
        scheduleTime2 = datetime.utcnow() + timedelta(hours=2)
        scheduleDuration2 = timedelta(minutes=30)
        db.insertSchedule(1, 4, scheduleTime1, scheduleDuration1, "show1", "episode1", "N")
        db.insertSchedule(38, 2, scheduleTime2, scheduleDuration2, "show1", "episode2", "R")
        pendingRecordings = db.getPendingRecordings()
        self.assertEqual(2, len(pendingRecordings))
        self.assertEqual(1, pendingRecordings[0].channelMajor)
        self.assertEqual(4, pendingRecordings[0].channelMinor)
        self.assertEqual(scheduleTime1, pendingRecordings[0].startTime)
        self.assertEqual(scheduleDuration1, pendingRecordings[0].duration)
        self.assertEqual("show1", pendingRecordings[0].showID)
        self.assertEqual("episode1", pendingRecordings[0].episodeID)
        self.assertEqual("N", pendingRecordings[0].rerunCode)
        self.assertEqual(38, pendingRecordings[1].channelMajor)
        self.assertEqual(2, pendingRecordings[1].channelMinor)
        self.assertEqual(scheduleTime2, pendingRecordings[1].startTime)
        self.assertEqual(scheduleDuration2, pendingRecordings[0].duration)
        self.assertEqual("show1", pendingRecordings[1].showID)
        self.assertEqual("episode2", pendingRecordings[1].episodeID)
        self.assertEqual("R", pendingRecordings[1].rerunCode)

        # if an episode is scheduled multiple times, only return the earliest
        db.insertSchedule(1, 1, scheduleTime1 - timedelta(minutes=20), scheduleDuration1, "show1", "episode1", "N")
        pendingRecordings = db.getPendingRecordings()
        self.assertEqual(2, len(pendingRecordings))
        self.assertEqual("show1", pendingRecordings[0].showID)
        self.assertEqual("episode1", pendingRecordings[0].episodeID)
        self.assertEqual(scheduleTime1 - timedelta(minutes=20), pendingRecordings[0].startTime)
        self.assertEqual("show1", pendingRecordings[1].showID)
        self.assertEqual("episode2", pendingRecordings[1].episodeID)
        self.assertEqual(scheduleTime2, pendingRecordings[1].startTime)

        # a video that already has a raw recording should not be recorded again
        db.insertRecording(1234, "show1", "episode1", datetime.utcnow() - timedelta(weeks=1), timedelta(minutes=30), 'N')
        db.insertRawFileLocation(1234, "foo.mp4")
        pendingRecordings = db.getPendingRecordings()
        self.assertEqual(1, len(pendingRecordings))
        self.assertEqual("show1", pendingRecordings[0].showID)
        self.assertEqual("episode2", pendingRecordings[0].episodeID)
        self.assertEqual(scheduleTime2, pendingRecordings[0].startTime)

        # a video that already has a transcoded recording should not be recorded again
        db.insertRecording(1235, "show1", "episode2", datetime.utcnow() - timedelta(weeks=1), timedelta(minutes=30), 'N')
        db.insertTranscodedFileLocation(1235, 1, "foo.mp4", 0)
        pendingRecordings = db.getPendingRecordings()
        self.assertEqual(0, len(pendingRecordings))

        # only subscribed shows should be returned
        db.insertShow(showID="show2", showType="comedy", name="Show #2")
        db.insertEpisode(showID="show2", episodeID="episode1", title="Show 2, Episode 1", description="")
        db.insertSchedule(1, 1, scheduleTime1, scheduleDuration1, "show2", "episode1", "N")
        self.assertEqual(0, len(db.getPendingRecordings()))
        db.insertSubscription(showID="show2", priority=1)
        self.assertEqual(1, len(db.getPendingRecordings()))
        db.deleteSubscription(showID="show2")
        self.assertEqual(0, len(db.getPendingRecordings()))

    def test_getRecordingsToBif(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        db.insertTranscodedFileLocation(1167, 0, "file1.mp4", db.TRANSCODE_SUCCESSFUL)
        db.insertTranscodedFileLocation(1235, 0, "file2.mp4", db.TRANSCODE_SUCCESSFUL)
        db.insertTranscodedFileLocation(1311, 0, "file3.mp4", db.TRANSCODE_FAILED)
        db.insertTranscodedFileLocation(1492, 0, "file4.mp4", db.TRANSCODE_SUCCESSFUL)
        db.insertBifFileLocation(1167, 1, "file1.bif")
        filesToBif = sorted(db.getRecordingsToBif(), key=lambda x:x.recordingID)
        self.assertEqual(2, len(filesToBif))
        self.assertEqual(1235, filesToBif[0].recordingID)
        self.assertEqual("file2.mp4", filesToBif[0].filename)
        self.assertEqual(1492, filesToBif[1].recordingID)
        self.assertEqual("file4.mp4", filesToBif[1].filename)

    def test_getUnreferencedRawVideoRecords(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        db.insertRecording(1234, "show1", "episode2", datetime.utcnow() - timedelta(weeks=1), timedelta(minutes=30), 'N')
        db.insertRawFileLocation(1234, "1234.mp4")
        db.insertRawFileLocation(1235, "1235.mp4")
        records = db.getUnreferencedRawVideoRecords()
        self.assertEqual(1, len(records))
        self.assertEqual(1235, records[0].recordingID)
        self.assertEqual("1235.mp4", records[0].filename)

    def test_deleteRawVideoRecord(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        db.insertRawFileLocation(1234, "1234.mp4")
        db.insertRawFileLocation(1235, "1235.mp4")
        records = sorted(db.getUnreferencedRawVideoRecords(), key=lambda x:x.recordingID)
        self.assertEqual(2, len(records))
        self.assertEqual(1234, records[0].recordingID)
        self.assertEqual(1235, records[1].recordingID)
        db.deleteRawVideoRecord(1234)
        records = db.getUnreferencedRawVideoRecords()
        self.assertEqual(1, len(records))
        self.assertEqual(1235, records[0].recordingID)

    def test_getUnreferencedTranscodedVideoRecords(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertRecording(1234, "show1", "episode2", datetime.utcnow() - timedelta(weeks=1), timedelta(minutes=30), 'N'))
        self.assertEqual(1, db.insertTranscodedFileLocation(1234, 0, "1234.mp4", db.TRANSCODE_SUCCESSFUL))
        self.assertEqual(1, db.insertTranscodedFileLocation(1235, 0, "1235.mp4", db.TRANSCODE_FAILED))
        records = db.getUnreferencedTranscodedVideoRecords()
        self.assertEqual(1, len(records))
        self.assertEqual(1235, records[0].recordingID)
        self.assertEqual("1235.mp4", records[0].filename)

    def test_deleteTranscodedVideoRecord(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertTranscodedFileLocation(1234, 0, "1234.mp4", db.TRANSCODE_SUCCESSFUL))
        self.assertEqual(1, db.insertTranscodedFileLocation(1235, 0, "1235.mp4", db.TRANSCODE_FAILED))
        records = db.getUnreferencedTranscodedVideoRecords()
        self.assertEqual(2, len(records))
        self.assertEqual(1234, records[0].recordingID)
        self.assertEqual(1235, records[1].recordingID)
        self.assertEqual(1, db.deleteTranscodedVideoRecord(1234))
        records = db.getUnreferencedTranscodedVideoRecords()
        self.assertEqual(1, len(records))
        self.assertEqual(1235, records[0].recordingID)

    def test_getUnreferencedBifRecords(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertRecording(1234, "show1", "episode2", datetime.utcnow() - timedelta(weeks=1), timedelta(minutes=30), 'N'))
        self.assertEqual(1, db.insertBifFileLocation(1234, 0, "1234.bif"))
        self.assertEqual(1, db.insertBifFileLocation(1235, 0, "1235.bif"))
        records = db.getUnreferencedBifRecords()
        self.assertEqual(1, len(records))
        self.assertEqual(1235, records[0].recordingID)
        self.assertEqual("1235.bif", records[0].filename)

    def test_deleteBifRecord(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertBifFileLocation(1234, 0, "1234.bif"))
        self.assertEqual(1, db.insertBifFileLocation(1235, 0, "1235.bif"))
        records = db.getUnreferencedBifRecords()
        self.assertEqual(2, len(records))
        self.assertEqual(1234, records[0].recordingID)
        self.assertEqual(1235, records[1].recordingID)
        self.assertEqual(1, db.deleteBifRecord(1234))
        records = db.getUnreferencedBifRecords()
        self.assertEqual(1, len(records))
        self.assertEqual(1235, records[0].recordingID)

    def test_getUnneededRawVideoRecords(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertRawFileLocation(1234, "1234.mpeg"))
        self.assertEqual(1, db.insertRawFileLocation(1235, "1235.mpeg"))
        self.assertEqual(1, db.insertRawFileLocation(1236, "1236.mpeg"))
        self.assertEqual(1, db.insertTranscodedFileLocation(1234, 0, "1234.mp4", db.TRANSCODE_FAILED))
        self.assertEqual(1, db.insertTranscodedFileLocation(1235, 0, "1235.mp4", db.TRANSCODE_SUCCESSFUL))
        records = db.getUnneededRawVideoRecords()
        self.assertEqual(1, len(records))
        self.assertEqual(1235, records[0].recordingID)
        self.assertEqual("1235.mpeg", records[0].filename)
        db.getUnneededRawVideoRecords()

    def test_selectRecordingsToTranscode(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertRawFileLocation(1234, "1234.mpeg2"))
        self.assertEqual(1, db.insertRawFileLocation(1235, "1235.mpeg2"))
        self.assertEqual(1, db.insertRawFileLocation(1236, "1236.mpeg2"))
        self.assertEqual(1, db.insertRawFileLocation(1237, "1237.mpeg2"))
        self.assertEqual(1, db.insertTranscodedFileLocation(1235, 0, "1235.mp4", db.TRANSCODE_SUCCESSFUL))
        self.assertEqual(1, db.insertTranscodedFileLocation(1236, 0, "1236.mp4", db.TRANSCODE_FAILED))
        recordingsToTranscode = sorted(db.selectRecordingsToTranscode(), key=lambda x:x.recordingID)
        self.assertEqual(2, len(recordingsToTranscode))
        self.assertEqual(1234, recordingsToTranscode[0].recordingID)
        self.assertEqual("1234.mpeg2", recordingsToTranscode[0].filename)
        self.assertEqual(1237, recordingsToTranscode[1].recordingID)
        self.assertEqual("1237.mpeg2", recordingsToTranscode[1].filename)

    def test_getDuration(self):
        db = SqliteDatabase(":memory:")
        db.initializeSchema()
        self.assertEqual(1, db.insertRecording(1234, "show1", "episode1", datetime.utcnow(), timedelta(minutes=30), 'N'))
        self.assertEqual(1, db.insertRecording(1235, "show1", "episode2", datetime.utcnow(), timedelta(minutes=59), 'N'))
        self.assertEqual(timedelta(minutes=30), db.getDuration(1234))
        self.assertEqual(timedelta(minutes=59), db.getDuration(1235))
        self.assertEqual(timedelta(seconds=0), db.getDuration(1236))

if __name__ == '__main__':
    unittest.main()  

