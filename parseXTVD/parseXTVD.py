#!/usr/bin/env python3.4

import logging
import psycopg2
from xml.etree import ElementTree


class carbonDVRDatabase:
    def __init__(self, dbConnection, schema):
        self.connection = dbConnection

    def commit(self):
        self.connection.commit()

    def insertShow(self, showID, showType, showName):
        numRowsInserted = 0
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM show WHERE show_id = %s", (showID, ))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO show(show_id, show_type, name) VALUES (%s, %s, %s)", (showID, showType, showName))
                numRowsInserted += cursor.rowcount
            else:
                cursor.execute("UPDATE show set show_type = %s, name = %s WHERE show_id = %s", (showType, showName, showID))
        return numRowsInserted

    def insertEpisode(self, showID, episodeID, episodeTitle, episodeDescription):
        numRowsInserted = 0
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM episode WHERE show_id = %s AND episode_id = %s", (showID, episodeID))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO episode(show_id, episode_id, title, description) VALUES (%s, %s, %s, %s)", (showID, episodeID, episodeTitle, episodeDescription))
                numRowsInserted += cursor.rowcount
        return numRowsInserted

    def insertSchedule(self, channelMajor, channelMinor, startTime, duration, showID, episodeID, rerunCode):
        numRowsInserted = 0
        with self.connection.cursor() as cursor:
            cursor.execute('INSERT INTO schedule(channel_major, channel_minor, start_time, duration, show_id, episode_id, rerun_code) VALUES (%s, %s, %s, %s, %s, %s, %s)',
                           (channelMajor, channelMinor, startTime, duration, showID, episodeID, rerunCode))
            numRowsInserted += cursor.rowcount
        return numRowsInserted

    def getChannels(self):
        channelSet = set()
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT major, minor FROM channel")
            for row in cursor:
                channelSet.add((row[0],row[1]))
        return channelSet

    def clearScheduleTable(self):
        numRowsDeleted = 0
        with self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM schedule")
            numRowsDeleted += cursor.rowcount
        return numRowsDeleted


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


class Station:
    pass


class Schedule:
    pass


class Program:
    pass


def extractStations(xmlElementTree):
    stations = {}
    for stationElement in xmlElementTree.getroot().findall(".//{urn:TMSWebServices}lineup/{urn:TMSWebServices}map"):
        station = Station()
        station.channelMajor = stationElement.attrib['channel']
        station.channelMinor = stationElement.attrib['channelMinor']
        stations[stationElement.attrib['station']] = station
    return stations


def extractSchedules(xmlElementTree, stationMap):
    schedules = []
    for scheduleElement in xmlElementTree.getroot().findall(".//{urn:TMSWebServices}schedules/{urn:TMSWebServices}schedule"):
        stationData = stationMap.get(scheduleElement.attrib['station'])
        if stationData:
            programID = ProgramID(scheduleElement.attrib['program'])
            schedule = Schedule()
            schedule.channelMajor = stationData.channelMajor
            schedule.channelMinor = stationData.channelMinor
            schedule.startTime = scheduleElement.attrib['time']
            schedule.duration = scheduleElement.attrib['duration']
            schedule.showID = programID.showID()
            schedule.episodeID = programID.episodeID()
            schedule.rerunCode = 'R'
            if scheduleElement.attrib.get('new') == 'true':
                schedule.rerunCode = 'N'
            schedules.append(schedule)
    return schedules


def extractPrograms(xmlElementTree):
    programs = []
    for programElement in xmlElementTree.getroot().findall(".//{urn:TMSWebServices}programs/{urn:TMSWebServices}program"):
        programID = ProgramID(programElement.attrib['id'])
        program = Program()
        program.showID = programID.showID()
        program.showType = programID.showType()
        program.series = programElement.findtext("{urn:TMSWebServices}series", "")
        program.showName = programElement.findtext("{urn:TMSWebServices}title", "")
        program.episodeID = programID.episodeID()
        program.episodeTitle = programElement.findtext("{urn:TMSWebServices}subtitle", "")
        program.episodeDescription = programElement.findtext("{urn:TMSWebServices}description", "")
        program.episodeNumber = programElement.findtext("{urn:TMSWebServices}syndicatedEpisodeNumber", "")
        programs.append(program)
    return programs


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

    programs = extractPrograms(xmlElementTree)
    if not programs:
        logger.error('No programs found.  Aborting.')
        return

    # insert records
    logger.info('Inserting shows')
    numShowsInserted = 0
    for program in programs:
        numShowsInserted += db.insertShow(program.showID, program.showType, program.showName);
    db.commit()
    logger.info('%d shows inserted', numShowsInserted)


    logger.info('Inserting episodes')
    numEpisodesInserted = 0
    for program in programs:
        numEpisodesInserted += db.insertEpisode(program.showID, program.episodeID, program.episodeTitle, program.episodeDescription)
    db.commit();
    logger.info('%d episodes inserted', numEpisodesInserted)

    logger.info('Clearing schedule table')
    numRowsDeleted = db.clearScheduleTable()
    logger.info('%s rows deleted from schedule table', numRowsDeleted)

    logger.info('Fetching channel list')
    channelSet = db.getChannels()
    logger.info('%s channels retrieved', len(channelSet))

    logger.info('Inserting schedules')
    numSchedulesAttempted = 0
    numSchedulesInserted = 0
    for schedule in extractSchedulesWithValidChannels(schedules, channelSet):
        numSchedulesAttempted += 1
        numSchedulesInserted += db.insertSchedule(schedule.channelMajor, schedule.channelMinor, schedule.startTime, schedule.duration, schedule.showID, schedule.episodeID, schedule.rerunCode)
    db.commit();
    logger.info('%d of %d schedules inserted', numSchedulesInserted, len(schedules))
    logger.info('%d schedules skipped (undefined channel)', len(schedules) - numSchedulesAttempted)
    logger.info('%d schedule inserts failed', numSchedulesAttempted - numSchedulesInserted)

