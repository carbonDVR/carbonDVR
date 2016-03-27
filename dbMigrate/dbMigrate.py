import logging
import psycopg2
import datetime
import parse


v1EpisodeIDParser = parse.compile('{:d}.{:d}/{:d}')

def stripPartCode(episodeID):
    p = v1EpisodeIDParser.parse(episodeID)
    if p is None:
        return episodeID
    return str(p[0])


def extractPartCode(episodeID):
    p = v1EpisodeIDParser.parse(episodeID)
    if p is None:
        return None
    if len(p.fixed) == 3:
        return '{}/{}'.format(p[1] + 1, p[2])
    return None


def migrate1to2(fromDB, fromSchema, toDB, toSchema):
    logger = logging.getLogger(__name__)

    shows=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT show_id, show_type, name, imageurl '
                        'FROM {fromSchema}.show').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                show = {}
                show['id'] = row[0]
                show['type'] = row[1]
                show['name'] = row[2]
                show['imageurl'] = row[3]
                shows.append(show)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for show in shows:
                query = 'SELECT show_id FROM {toSchema}.show WHERE show_id=%s'.format(toSchema=toSchema)
                cursor.execute(query, (show['id'], ))
                if cursor.rowcount == 0:
                    query = str('INSERT INTO {toSchema}.show (show_id, show_type, name, imageurl) '
                                'VALUES (%s, %s, %s, %s);').format(toSchema=toSchema)
                    cursor.execute(query, (show['id'], show['type'], show['name'], show['imageurl']))
                    numInserts += cursor.rowcount
        logger.info('Shows inserted: %d', numInserts)


    subscriptions=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT show_id '
                        'FROM {fromSchema}.subscription').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                subscription = {}
                subscription['show_id'] = row[0]
                subscriptions.append(subscription)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for subscription in subscriptions:
                query = 'SELECT show_id FROM {toSchema}.subscription WHERE show_id=%s'.format(toSchema=toSchema)
                cursor.execute(query, (subscription['show_id'], ))
                if cursor.rowcount == 0:
                    query = str('INSERT INTO {toSchema}.subscription (show_id) '
                                'VALUES (%s);').format(toSchema=toSchema)
                    cursor.execute(query, (subscription['show_id'],))
                    numInserts += cursor.rowcount
        logger.info('Subscriptions inserted: %d', numInserts)


    episodes=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT show_id, episode_id, title, description, imageurl '
                        'FROM {fromSchema}.episode').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                episode = {}
                episode['show_id'] = row[0]
                episode['episode_id'] = stripPartCode(row[1])
                episode['title'] = row[2]
                episode['description'] = row[3]
                episode['part_code'] = extractPartCode(row[1])
                episode['imageurl'] = row[4]
                episodes.append(episode)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for episode in episodes:
                query = 'SELECT show_id, episode_id FROM {toSchema}.episode WHERE show_id=%s AND episode_id=%s'.format(toSchema=toSchema)
                cursor.execute(query, (episode['show_id'], episode['episode_id']))
                if cursor.rowcount == 0:
                    query = str('INSERT INTO {toSchema}.episode (show_id, episode_id, title, description, part_code, imageurl) '
                                'VALUES (%s, %s, %s, %s, %s, %s);').format(toSchema=toSchema)
                    cursor.execute(query, (episode['show_id'], episode['episode_id'], episode['title'], episode['description'], episode['part_code'], episode['imageurl']))
                    numInserts += cursor.rowcount
        logger.info('Episodes inserted: %d', numInserts)


    channels=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str("SELECT cast(split_part(channel_id, '-', 1) as integer), cast(split_part(channel_id, '-', 2) as integer) "
                        "FROM {fromSchema}.channel").format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                channel = {}
                channel['major'] = row[0]
                channel['minor'] = row[1]
                channels.append(channel)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for channel in channels:
                query = 'SELECT major, minor FROM {toSchema}.channel WHERE major=%s AND minor=%s'.format(toSchema=toSchema)
                cursor.execute(query, (channel['major'], channel['minor']))
                if cursor.rowcount == 0:
                    query = str('INSERT INTO {toSchema}.channel (major, minor) '
                                'VALUES (%s, %s);').format(toSchema=toSchema)
                    cursor.execute(query, (channel['major'], channel['minor']))
                    numInserts += cursor.rowcount
        logger.info('Channels inserted: %d', numInserts)


    recordings=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT recording_id, show_id, episode_id, date_recorded, duration, category_code '
                        'FROM {fromSchema}.recording').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                recording = {}
                recording['recording_id'] = row[0]
                recording['show_id'] = row[1]
                recording['episode_id'] = stripPartCode(row[2])
                recording['date_recorded'] = row[3]
                recording['duration'] = row[4]
                recording['rerun_code'] = row[5]
                recordings.append(recording)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for recording in recordings:
                query = str('INSERT INTO {toSchema}.recording (recording_id, show_id, episode_id, date_recorded, duration, rerun_code) '
                            'VALUES (%s, %s, %s, %s, %s, %s);').format(toSchema=toSchema)
                cursor.execute(query, (recording['recording_id'], recording['show_id'], recording['episode_id'], recording['date_recorded'], recording['duration'], recording['rerun_code']))
                numInserts += cursor.rowcount
        logger.info('Recordings inserted: %d', numInserts)


    files=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT recording_id, filename '
                        'FROM {fromSchema}.file_raw_video').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                file = {}
                file['recording_id'] = row[0]
                file['filename'] = row[1]
                files.append(file)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for file in files:
                query = str('INSERT INTO {toSchema}.file_raw_video (recording_id, filename) '
                            'VALUES (%s, %s);').format(toSchema=toSchema)
                cursor.execute(query, (file['recording_id'], file['filename']))
                numInserts += cursor.rowcount
        logger.info('Raw video file locations inserted: %d', numInserts)


    files=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT recording_id, filename, state '
                        'FROM {fromSchema}.file_transcoded_video').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                file = {}
                file['recording_id'] = row[0]
                file['filename'] = row[1]
                file['state'] = row[2]
                files.append(file)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for file in files:
                query = str('INSERT INTO {toSchema}.file_transcoded_video (recording_id, filename, state) '
                            'VALUES (%s, %s, %s);').format(toSchema=toSchema)
                cursor.execute(query, (file['recording_id'], file['filename'], file['state']))
                numInserts += cursor.rowcount
        logger.info('Transcode file locations inserted: %d', numInserts)


    files=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT recording_id, filename '
                        'FROM {fromSchema}.file_bif').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                file = {}
                file['recording_id'] = row[0]
                file['filename'] = row[1]
                files.append(file)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for file in files:
                query = str('INSERT INTO {toSchema}.file_bif (recording_id, filename) '
                            'VALUES (%s, %s);').format(toSchema=toSchema)
                cursor.execute(query, (file['recording_id'], file['filename']))
                numInserts += cursor.rowcount
        logger.info('BIF file locations inserted: %d', numInserts)



    records=[]
    with fromDB:
        with fromDB.cursor() as cursor:
            query = str('SELECT recording_id, position '
                        'FROM {fromSchema}.playback_position').format(fromSchema=fromSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)
            for row in cursor:
                record = {}
                record['recording_id'] = row[0]
                record['position'] = row[1]
                records.append(record)
    with toDB:
        numInserts = 0
        with toDB.cursor() as cursor:
            for record in records:
                query = str('INSERT INTO {toSchema}.playback_position (recording_id, position) '
                            'VALUES (%s, %s);').format(toSchema=toSchema)
                cursor.execute(query, (record['recording_id'], record['position']))
                numInserts += cursor.rowcount
        logger.info('Playback position records inserts: %d', numInserts)


