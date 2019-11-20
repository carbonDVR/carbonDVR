#!/usr/bin/env python3

import argparse
import logging
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))
from bunch import Bunch
import isodate
from sqliteDatabase import SqliteDatabase
from xml.etree import ElementTree


# helper class to extract Show Type, Show ID, and Episode ID from a Program ID
class ProgramID:
    def __init__(self, programID):
        self.programID = programID

    def showType(self):
        return self.programID[0:2]
   
    def showID(self):
        showID = self.programID[2:10].lstrip('0')
        if showID:
          return showID
        return '0'
   
    def episodeID(self):
        episodeID = self.programID[10:14].lstrip('0')
        if episodeID:
          return episodeID
        return '0'


def extractStations(xmlElementTree):
    stations = {}
    for stationElement in xmlElementTree.getroot().findall(".//{urn:TMSWebServices}lineup/{urn:TMSWebServices}map"):
        station = Bunch(
            channelMajor=stationElement.attrib['channel'],
            channelMinor=stationElement.attrib['channelMinor'])
        stations[stationElement.attrib['station']] = station
    return stations


def extractSchedules(xmlElementTree, stationMap):
    schedules = []
    for scheduleElement in xmlElementTree.getroot().findall(".//{urn:TMSWebServices}schedules/{urn:TMSWebServices}schedule"):
        stationData = stationMap.get(scheduleElement.attrib['station'])
        if stationData:
            programID = ProgramID(scheduleElement.attrib['program'])
            rerunCode = 'R'
            if scheduleElement.attrib.get('new') == 'true':
                rerunCode = 'N'
            schedule = Bunch(
                channelMajor=stationData.channelMajor,
                channelMinor=stationData.channelMinor,
                startTime=isodate.parse_datetime(scheduleElement.attrib['time']),
                duration=isodate.parse_duration(scheduleElement.attrib['duration']),
                showID=programID.showID(),
                episodeID=programID.episodeID(),
                rerunCode=rerunCode)
            schedules.append(schedule)
    return schedules


def extractPartCodes(xmlElementTree, stationMap):
    partCodes = {}
    for scheduleElement in xmlElementTree.getroot().findall(".//{urn:TMSWebServices}schedules/{urn:TMSWebServices}schedule"):
        partElement = scheduleElement.find(".//{urn:TMSWebServices}part")
        if partElement is not None:
            partCode = '{}/{}'.format(partElement.attrib['number'], partElement.attrib['total'])
            programID = scheduleElement.attrib['program']
            partCodes[programID] = partCode
    return partCodes


def extractPrograms(xmlElementTree):
    programs = []
    for programElement in xmlElementTree.getroot().findall(".//{urn:TMSWebServices}programs/{urn:TMSWebServices}program"):
        programID = ProgramID(programElement.attrib['id'])
        program = Bunch(
            programID=programElement.attrib['id'],
            showID=programID.showID(),
            showType=programID.showType(),
            series=programElement.findtext("{urn:TMSWebServices}series", ""),
            showName=programElement.findtext("{urn:TMSWebServices}title", ""),
            episodeID=programID.episodeID(),
            episodeTitle=programElement.findtext("{urn:TMSWebServices}subtitle", ""),
            episodeDescription=programElement.findtext("{urn:TMSWebServices}description", ""),
            episodeNumber=programElement.findtext("{urn:TMSWebServices}syndicatedEpisodeNumber", ""))
        programs.append(program)
    return programs


def addPartCodes(programs, partCodes):
    for program in programs:
        program.partCode = partCodes.get(program.programID)
        yield program


def extractSchedulesWithValidChannels(schedules, channelSet):
    for schedule in schedules:
      channelMajor = int(schedule.channelMajor)
      channelMinor = int(schedule.channelMinor)
      if (channelMajor, channelMinor) in channelSet:
        yield schedule


def parseXTVD(xtvdFile, db):
    logger = logging.getLogger(__name__)
    logger.info('Parsing file "%s"', xtvdFile)
    xmlElementTree = ElementTree.parse(xtvdFile)
    logger.info('Finished parsing file "%s"', xtvdFile)

    # process XML
    stations = extractStations(xmlElementTree)
    if not stations:
        logger.error('No stations found.  Aborting.')
        return

    schedules = extractSchedules(xmlElementTree, stations)
    if not schedules:
        logger.error('No schedules found.  Aborting.')
        return

    partCodes = extractPartCodes(xmlElementTree, stations)

    programs = extractPrograms(xmlElementTree)
    if not programs:
        logger.error('No programs found.  Aborting.')
        return

    # insert records
    logger.info('Inserting shows')
    numShowsInserted, numShowsUpdated = db.insertShows(programs)
    logger.info('%d shows inserted', numShowsInserted)
    logger.info('%d shows updated', numShowsUpdated)

    logger.info('Inserting episodes')
    numEpisodesInserted = db.insertEpisodes(programs)
    logger.info('%d episodes inserted', numEpisodesInserted)

    logger.info('Clearing schedule table')
    numRowsDeleted = db.clearScheduleTable()
    logger.info('%s rows deleted from schedule table', numRowsDeleted)

    logger.info('Fetching channel list')
    channels = db.getChannels()
    channelSet = { (int(x.channelMajor), int(x.channelMinor)) for x in channels }
    logger.info('%s channels retrieved', len(channelSet))

    logger.info('Inserting schedules')
    validSchedules = list(extractSchedulesWithValidChannels(schedules, channelSet))
    numSchedulesInserted = db.insertSchedules(validSchedules)
    logger.info('%d of %d schedules inserted', numSchedulesInserted, len(schedules))
    logger.info('%d schedules skipped (undefined channel)', len(schedules) - len(validSchedules))
    logger.info('%d schedule inserts failed', len(validSchedules) - numSchedulesInserted)



if __name__ == '__main__':
    FORMAT = '%(asctime)-15s: %(name)s:  %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Parse listings in XTVD format.')
    parser.add_argument('-x', '--xtvdFile', dest='xtvdFile', default='/opt/carbonDVR/var/listings.xtvd')
    parser.add_argument('-d', '--dbFile', dest='dbFile', default='/opt/carbonDVR/lib/carbonDVR.sqlite')
    args = parser.parse_args()

    dbConnection = SqliteDatabase(args.dbFile)
    parseXTVD(args.xtvdFile, dbConnection)

