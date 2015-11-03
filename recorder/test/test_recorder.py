import pytz
import unittest
from carbonDVRDatabase import CarbonDVRDatabase
from hdhomerun import HDHomeRunInterface, BadRecordingException
from recorder import Recorder
from datetime import datetime,timedelta
from unittest.mock import Mock, call


class Bunch:
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class TestRecorder(unittest.TestCase):

    def test_recorder_removeAllRecordingJobs(self):
        hdhomerun = Mock(HDHomeRunInterface)
        db = Mock(CarbonDVRDatabase)
        recorder = Recorder(hdhomerun, db, 'recs', 'logs')
        recorder.logger = Mock()
        recorder.scheduler = Mock()
        # given: a mix of recording jobs and non-recording jobs
        recorder.scheduler.get_jobs.return_value = [ Bunch(func=recorder.record, id=3),
                                                     Bunch(func=recorder.scheduleJobs, id=1),
                                                     Bunch(func=recorder.record, id=4) ]
        # when: removeAllRecordingJobs
        recorder.removeAllRecordingJobs()
        # then: all recording jobs removed, non-recording jobs ignored
        self.assertEqual(2, recorder.scheduler.remove_job.call_count)
        self.assertEqual(recorder.scheduler.remove_job.call_args_list[0], call(3))
        self.assertEqual(recorder.scheduler.remove_job.call_args_list[1], call(4))

    def test_recorder_scheduleJobs(self):
        hdhomerun = Mock(HDHomeRunInterface)
        db = Mock(CarbonDVRDatabase)
        recorder = Recorder(hdhomerun, db, 'recs', 'logs')
        recorder.logger = Mock()
        recorder.scheduler = Mock()
        recorder.removeAllRecordingJobs = Mock()
        # given: a set of pending recordings
        mockPendingRecordings = [ Bunch(channelMajor=1, channelMinor=2, startTime=datetime(2000,1,1,12,00,00, tzinfo=pytz.timezone('US/Central'))),
                                  Bunch(channelMajor=19, channelMinor=3, startTime=datetime(2000,1,1,13,00,00, tzinfo=pytz.timezone('US/Central'))),
                                  Bunch(channelMajor=38, channelMinor=1, startTime=datetime(2000,1,1,14,00,00, tzinfo=pytz.timezone('US/Central'))) ]
        db.getPendingRecordings.return_value = mockPendingRecordings
        # when: scheduleJobs
        recorder.scheduleJobs()
        # then: all existing recording jobs cleared, getPendingRecordings is called, new recording jobs are added
        recorder.removeAllRecordingJobs.assert_called_once()
        db.getPendingRecordings.assert_called_once_with(timedelta(hours=12))
        self.assertEqual(3, recorder.scheduler.add_job.call_count)
        call0 = call(recorder.record, args=[mockPendingRecordings[0]], trigger='date', run_date=mockPendingRecordings[0].startTime, misfire_grace_time=60)
        self.assertEqual(recorder.scheduler.add_job.call_args_list[0], call0)
        call1 = call(recorder.record, args=[mockPendingRecordings[1]], trigger='date', run_date=mockPendingRecordings[1].startTime, misfire_grace_time=60)
        self.assertEqual(recorder.scheduler.add_job.call_args_list[1], call1)
        call2 = call(recorder.record, args=[mockPendingRecordings[2]], trigger='date', run_date=mockPendingRecordings[2].startTime, misfire_grace_time=60)
        self.assertEqual(recorder.scheduler.add_job.call_args_list[2], call2)

    def test_recorder_record_success(self):
        hdhomerun = Mock(HDHomeRunInterface)
        db = Mock(CarbonDVRDatabase)
        recorder = Recorder(hdhomerun, db, 'rec/recording_{recordingID}.mp4', 'logs/recording_{recordingID}.log')
        recorder.logger = Mock()
        schedule = Bunch(channelMajor=1, channelMinor=2, startTime=datetime(1970,1,1,0,0,0), duration=timedelta(minutes=47), showID='show1', episodeID='episode1', rerunCode='R')
        db.getUniqueID.return_value = 3
        recorder.record(schedule)
        db.insertRecording.assert_called_once_with(3, 'show1', 'episode1', timedelta(minutes=47), 'R')
        hdhomerun.record.assert_called_once_with(1, 2, datetime(1970,1,1,0,0,0) + timedelta(minutes=47), 'rec/recording_3.mp4', 'logs/recording_3.log')
        db.insertRawVideoLocation.assert_called_once_with(3, 'rec/recording_3.mp4')

    def test_recorder_record_fail(self):
        hdhomerun = Mock(HDHomeRunInterface)
        db = Mock(CarbonDVRDatabase)
        recorder = Recorder(hdhomerun, db, '/var/spool/carbondvr/recordings/raw_{recordingID}.mp4', '/var/log/carbondvr/recordings/rec{recordingID}.log')
        recorder.logger = Mock()
        schedule = Bunch(channelMajor=8, channelMinor=3, startTime=datetime(1992,12,21,16,57,19), duration=timedelta(minutes=15), showID='show2', episodeID='episode2', rerunCode='N')
        db.getUniqueID.return_value = 58162
        hdhomerun.record.side_effect=BadRecordingException()
        recorder.record(schedule)
        db.insertRecording.assert_called_once_with(58162, 'show2', 'episode2', timedelta(minutes=15), 'N')
        hdhomerun.record.assert_called_once_with(8, 3, datetime(1992,12,21,16,57,19) + timedelta(minutes=15),
                                                 '/var/spool/carbondvr/recordings/raw_58162.mp4', '/var/log/carbondvr/recordings/rec58162.log')
        self.assertFalse(db.insertRawVideoLocation.called)


if __name__ == '__main__':
    unittest.main()  

