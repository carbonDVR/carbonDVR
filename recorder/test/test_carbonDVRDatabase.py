import unittest
import os
import psycopg2
#from carbonDVRDatabase import CarbonDVRDatabase
from recorder import CarbonDVRDatabase
from datetime import timedelta


def isDatabaseConfigPresent():
    if os.environ.get('TEST_DB_CONNECT_STRING') and os.environ.get('TEST_DB_SCHEMA'):
        return True
    return False


@unittest.skipUnless(isDatabaseConfigPresent(), 'No test database configured')
class TestDatabase(unittest.TestCase):

    def setUp(self):
        dbConnectString = os.environ.get('TEST_DB_CONNECT_STRING')
        if dbConnectString is None:
            raise RuntimeError('TEST_DB_CONNECT_STRING environment variable is not set')
        self.dbConnection = psycopg2.connect(dbConnectString)
        self.dbConnection.autocommit = True

        schema = os.environ.get('TEST_DB_SCHEMA')
        if schema is not None:
            with self.dbConnection.cursor() as cursor:
                cursor.execute("SET SCHEMA %s", (schema, ))

        self.clearDatabase()

    def tearDown(self):
        self.dbConnection.close()

    def clearDatabase(self):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("DELETE FROM file_raw_video")
            cursor.execute("DELETE FROM recording")
            cursor.execute("DELETE FROM schedule")
            cursor.execute("DELETE FROM episode")
            cursor.execute("DELETE FROM subscription")
            cursor.execute("DELETE FROM show")
            cursor.execute("DELETE FROM tuner")
            cursor.execute("DELETE FROM channel")

    def insertShow(self, showID, showType, name):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO show(show_id, show_type, name) VALUES (%s, %s, %s)", (showID, showType, name))
            return cursor.rowcount

    def insertSubscription(self, showID, priority):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO subscription(show_id, priority) VALUES (%s, %s)", (showID, priority))
            return cursor.rowcount

    def insertEpisode(self, showID, episodeID, title, description):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO episode(show_id, episode_id, title, description) VALUES (%s, %s, %s, %s)", (showID, episodeID, title, description))
            return cursor.rowcount

    def insertRecording(self, recordingID, showID, episodeID, dateRecorded, duration, rerunCode):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO recording(recording_id, show_id, episode_id, date_recorded, duration, rerun_code) VALUES (%s, %s, %s, %s, %s, %s)",
                           (recordingID, showID, episodeID, dateRecorded, duration, rerunCode))
            return cursor.rowcount

    def insertTuner(self, deviceID, ipaddress, tunerID):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO tuner(device_id, ipaddress, tuner_id) VALUES (%s, %s, %s)", (deviceID, ipaddress, tunerID))
            return cursor.rowcount

    def insertChannel(self, channelMajor, channelMinor, channelActual, program):
        with self.dbConnection.cursor() as cursor:
            cursor.execute("INSERT INTO channel(major, minor, actual, program) VALUES (%s, %s, %s, %s)", (channelMajor, channelMinor, channelActual, program))
            return cursor.rowcount

    def test_carbonDVRDatabase_getTuners(self):
        self.assertEqual(1, self.insertTuner('foo','192.168.1.1',1))
        self.assertEqual(1, self.insertTuner('baz','10.10.10.1',0))
        db = CarbonDVRDatabase(self.dbConnection)
        tuners = sorted(db.getTuners(), key=lambda tuner:tuner.deviceID)
        self.assertEqual(2, len(tuners))
        self.assertEqual('baz', tuners[0].deviceID)
        self.assertEqual('10.10.10.1', tuners[0].ipAddress)
        self.assertEqual(0, tuners[0].tunerID)
        self.assertEqual('foo', tuners[1].deviceID)
        self.assertEqual('192.168.1.1', tuners[1].ipAddress)
        self.assertEqual(1, tuners[1].tunerID)
        
    def test_carbonDVRDatabase_getChannels(self):
        self.assertEqual(1, self.insertChannel(4,1,5,1))
        self.assertEqual(1, self.insertChannel(19,3,18,2))
        self.assertEqual(1, self.insertChannel(5,2,37,3))
        db = CarbonDVRDatabase(self.dbConnection)
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

    # trivial 'does it throw an exception' test
    def test_carbonDVRDatabase_getPendingRecordings(self):
        db = CarbonDVRDatabase(self.dbConnection)
        pendingRecordings = db.getPendingRecordings(timedelta(hours=12))
        
    # trivial 'does it throw an exception' test
    def test_carbonDVRDatabase_insertRecording(self):
        self.insertShow('show','EP','foo')
        self.insertEpisode('show','episode','foo','foo')
        db = CarbonDVRDatabase(self.dbConnection)
        rowsInserted = db.insertRecording(recordingID='1',showID='show', episodeID='episode',duration='1',rerunCode='R')
        
    # trivial 'does it throw an exception' test
    def test_carbonDVRDatabase_insertRawVideoLocation(self):
        self.insertShow('show','EP','foo')
        self.insertEpisode('show','episode','foo','foo')
        self.insertRecording(1,'show','episode', '1970-01-01', timedelta(minutes=30), 'R')
        db = CarbonDVRDatabase(self.dbConnection)
        rowsInserted = db.insertRawVideoLocation(recordingID='1',filename='1')


#def main():
#    if not os.environ.get('TEST_DB_CONNECT_STRING'):
#        return    
#    if not os.environ.get('TEST_DB_SCHEMA'):
#        return
#    unittest.main()  



#if __name__ == '__main__':
#    main()

