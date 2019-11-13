import os
import sqlite3
import unittest
from sqliteDatabase import SqliteDatabase
from datetime import datetime, timedelta


class TestDatabase(unittest.TestCase):
    def test_carbonDVRDatabase_getTuners(self):
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
        
    def test_carbonDVRDatabase_getChannels(self):
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

    def test_carbonDVRDatabase_getPendingRecordings(self):
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
        db.insertSchedule(1, 1, scheduleTime1, scheduleDuration1, "show1", "episode1", "N")
        db.insertSchedule(1, 1, scheduleTime2, scheduleDuration2, "show1", "episode2", "N")
        pendingRecordings = db.getPendingRecordings()
        self.assertEqual(2, len(pendingRecordings))
        self.assertEqual("show1", pendingRecordings[0].showID)
        self.assertEqual("episode1", pendingRecordings[0].episodeID)
        self.assertEqual(scheduleTime1, pendingRecordings[0].startTime)
        self.assertEqual("show1", pendingRecordings[1].showID)
        self.assertEqual("episode2", pendingRecordings[1].episodeID)
        self.assertEqual(scheduleTime2, pendingRecordings[1].startTime)

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


if __name__ == '__main__':
    unittest.main()  

