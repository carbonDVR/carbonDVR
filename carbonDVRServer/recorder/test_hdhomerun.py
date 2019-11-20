import pytz
import unittest
from bunch import Bunch
from datetime import datetime
from hdhomerun import ChannelMap, TunerList, HDHomeRunInterface, UnrecognizedChannelException, NoTunersAvailableException, BadRecordingException
from unittest.mock import Mock, patch, DEFAULT




class TestChannelMap(unittest.TestCase):

    def test_channelmap_getChannelInfo(self):
        channels = [ Bunch(channelMajor=11, channelMinor=3, channelActual=24, program=2),
                     Bunch(channelMajor=36, channelMinor=2, channelActual=82, program=1),
                     Bunch(channelMajor=25, channelMinor=1, channelActual=7, program=3) ]
        channelMap = ChannelMap(channels)
        self.assertEqual(3, len(channelMap.channelDict))
        retrievedInfo = channelMap.getChannelInfo(25,1)
        self.assertEqual(7, retrievedInfo.channelActual)
        self.assertEqual(3, retrievedInfo.program)
        self.assertIsNone(channelMap.getChannelInfo(25,2))


class TestTunerList(unittest.TestCase):

    def setUp(self):
        self.tunerA = Bunch(deviceID='A', ipAddress='127.0.0.1', tunerID=1)
        self.tunerB = Bunch(deviceID='B', ipAddress='127.0.0.1', tunerID=0)
        self.tunerC = Bunch(deviceID='C', ipAddress='127.0.0.1', tunerID=2)
        self.tunerD = Bunch(deviceID='D', ipAddress='127.0.0.1', tunerID=1)
        self.tunerE = Bunch(deviceID='E', ipAddress='127.0.0.1', tunerID=0)

    def test_tunerlist_lockTuner(self):
        tuners = [ self.tunerA, self.tunerB, self.tunerC]
        tunerList = TunerList(tuners)
        lockedTuner = tunerList.lockTuner()
        self.assertEqual(self.tunerA, lockedTuner)
        self.assertEqual(2, len(tunerList.tuners))
        self.assertEqual(self.tunerB, tunerList.tuners[0])
        self.assertEqual(self.tunerC, tunerList.tuners[1])
        self.assertEqual(1, len(tunerList.lockedTuners))
        self.assertEqual(self.tunerA, tunerList.lockedTuners[0])

    def test_tunerlist_releaseTuner(self):
        tunerList = TunerList([])
        tunerList.tuners = [self.tunerB, self.tunerA]
        tunerList.lockedTuners = [self.tunerE, self.tunerC, self.tunerD]
        tunerList.releaseTuner(self.tunerC)
        self.assertEqual(3, len(tunerList.tuners))
        self.assertEqual(self.tunerB, tunerList.tuners[0])
        self.assertEqual(self.tunerA, tunerList.tuners[1])
        self.assertEqual(self.tunerC, tunerList.tuners[2])
        self.assertEqual(2, len(tunerList.lockedTuners))
        self.assertEqual(self.tunerE, tunerList.lockedTuners[0])
        self.assertEqual(self.tunerD, tunerList.lockedTuners[1])

    def test_tunerlist_releaseTuner_notLocked(self):
        tunerList = TunerList(tunerList=[self.tunerA, self.tunerB, self.tunerC])
        self.assertEqual(self.tunerA, tunerList.lockTuner())
        self.assertEqual(self.tunerB, tunerList.lockTuner())
        self.assertEqual(tunerList.tuners, [self.tunerC])
        self.assertEqual(tunerList.lockedTuners, [self.tunerA, self.tunerB])
        tunerList.releaseTuner(self.tunerC) # tunerC is not locked!
        self.assertEqual(tunerList.tuners, [self.tunerC])
        self.assertEqual(tunerList.lockedTuners, [self.tunerA, self.tunerB])

    def test_tunerlist_releaseTuner_invalid(self):
        tunerList = TunerList(tunerList=[self.tunerB, self.tunerC])
        self.assertEqual(tunerList.tuners, [self.tunerB, self.tunerC])
        self.assertFalse(tunerList.lockedTuners)
        tunerList.releaseTuner(self.tunerA) # not managed by tunerList
        self.assertEqual(tunerList.tuners, [self.tunerB, self.tunerC])
        self.assertFalse(tunerList.lockedTuners)

    def test_tunerlist_complexTest(self):
        tunerList = TunerList(tunerList=[self.tunerA, self.tunerB, self.tunerC, self.tunerD])
        self.assertEqual(self.tunerA, tunerList.lockTuner())
        self.assertEqual(self.tunerB, tunerList.lockTuner())
        self.assertEqual(self.tunerC, tunerList.lockTuner())
        self.assertEqual(self.tunerD, tunerList.lockTuner())
        self.assertIsNone(tunerList.lockTuner())
        self.assertEqual(tunerList.lockedTuners, [self.tunerA, self.tunerB, self.tunerC, self.tunerD])
        tunerList.releaseTuner(self.tunerE) # not managed by tunerList
        tunerList.releaseTuner(self.tunerC)
        tunerList.releaseTuner(self.tunerB)
        self.assertEqual(tunerList.tuners, [self.tunerC, self.tunerB])
        self.assertEqual(self.tunerC, tunerList.lockTuner())
        tunerList.releaseTuner(self.tunerA)
        tunerList.releaseTuner(self.tunerD)
        tunerList.releaseTuner(self.tunerC)
        self.assertFalse(tunerList.lockedTuners)
        self.assertEqual(tunerList.tuners, [self.tunerB, self.tunerA, self.tunerD, self.tunerC])


class TestHDHomeRunInterface(unittest.TestCase):

    def setUp(self):
        self.tunerA = Bunch(deviceID='A', ipAddress='127.0.0.1', tunerID=1)
        self.tunerB = Bunch(deviceID='B', ipAddress='127.0.0.1', tunerID=0)
        self.tunerC = Bunch(deviceID='C', ipAddress='127.0.0.1', tunerID=2)
        self.channelA = Bunch(channelMajor=11, channelMinor=3, channelActual=82, program=1)
        self.channelB = Bunch(channelMajor=37, channelMinor=1, channelActual=16, program=2)
        self.stoptime = datetime(2001, 4, 21, hour=12, tzinfo=pytz.utc)

    # trivial test which basically just looks for syntax errors
    def test_hdhomeruninterface_record_syntaxcheck(self):
        with patch.multiple('hdhomerun', io=DEFAULT, os=DEFAULT, subprocess=DEFAULT, time=DEFAULT, isaValidRecording=DEFAULT) as patchMocks:
            hdhomerunInterface = HDHomeRunInterface([], [], '/bin/false')
            hdhomerunInterface.logger = Mock()
            hdhomerunInterface.channelMap.getChannelInfo = Mock(autospec=True, return_value=self.channelA)
            hdhomerunInterface.tunerList.lockTuner = Mock(autospec=True, return_value=self.tunerA)
            hdhomerunInterface.record(self.channelA.channelMajor, self.channelA.channelMinor, self.stoptime, '/tmp/', '/tmp/')

    def test_hdhomeruninterface_record_badChannel(self):
        hdhomerun = HDHomeRunInterface([], [], '/bin/false')
        hdhomerun.logger = Mock()
        hdhomerun.channelMap.getChannel = Mock(return_value=None)
        hdhomerun.tunerList.lockTuner = Mock(return_value=self.tunerA)
        with self.assertRaises(UnrecognizedChannelException):
            hdhomerun.record(36, 1, self.stoptime, '/tmp/', '/tmp/')

    def test_hdhomeruninterface_record_noTuners(self):
        hdhomerun = HDHomeRunInterface([], [], '/bin/false')
        hdhomerun.logger = Mock()
        hdhomerun.channelMap.getChannelInfo = Mock(autospec=True, return_value=self.channelA)
        hdhomerun.tunerList.lockTuner = Mock(autospec=True, return_value=None)
        with self.assertRaises(NoTunersAvailableException):
            hdhomerun.record(self.channelA.channelMajor, self.channelA.channelMinor, self.stoptime, '/tmp/', '/tmp/')

