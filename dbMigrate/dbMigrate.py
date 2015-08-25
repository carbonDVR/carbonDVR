import logging
import psycopg2


def migrate1to2(dbConnection, fromSchema, toSchema, replaceToSchema):
    logger = logging.getLogger(__name__)
    with dbConnection.cursor() as cursor:

        if replaceToSchema:
            query = str('DROP SCHEMA {toSchema} CASCADE').format(toSchema=toSchema)
            logger.info('Query: %s', query)
            cursor.execute(query)

        query = str('CREATE SCHEMA {toSchema}').format(toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)


        query = str('CREATE TABLE {toSchema}.show ('
                    'show_id        text PRIMARY KEY,'
                    'show_type      character(2),'
                    'name           text,'
                    'imageurl       text)').format(toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)
     
        query = str('INSERT INTO {toSchema}.show (show_id, show_type, name, imageurl) '
                    'SELECT show_id, show_type, name, imageurl '
                    'FROM {fromSchema}.show').format(fromSchema=fromSchema, toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)
        logger.info('Rows inserted: %d', cursor.rowcount)


        query = str('CREATE TABLE {toSchema}.episode ('
                    'show_id        text,'
                    'episode_id     text,'
                    'title          text,'
                    'description    text,'
                    'imageurl       text,'
                    'PRIMARY KEY (show_id, episode_id),'
                    'FOREIGN KEY (show_id) REFERENCES {toSchema}.show(show_id))').format(toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)
     
        query = str('INSERT INTO {toSchema}.episode (show_id, episode_id, title, description, imageurl) '
                    'SELECT show_id, episode_id, title, description, imageurl '
                    'FROM {fromSchema}.episode').format(fromSchema=fromSchema, toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)
        logger.info('Rows inserted: %d', cursor.rowcount)


        query = str('CREATE TABLE {toSchema}.channel ('
                    'major          integer,'
                    'minor          integer,'
                    'actual         integer,'
                    'program        integer,'
                    'PRIMARY KEY (major, minor))').format(toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)
     
        query = str("INSERT INTO {toSchema}.channel (major, minor) "
                    "SELECT cast(split_part(channel_id, '-', 1) as integer), cast(split_part(channel_id, '-', 2) as integer) "
                    "FROM {fromSchema}.channel").format(fromSchema=fromSchema, toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)
        logger.info('Rows inserted: %d', cursor.rowcount)


        query = str('CREATE TABLE {toSchema}.tuner ('
                    'device_id      text,'
                    'ipaddress      inet,'
                    'tuner_id       integer)').format(toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)


        query = str('CREATE TABLE {toSchema}.schedule ('
                   'schedule_id    SERIAL PRIMARY KEY,'
                   'channel_major  integer,'
                   'channel_minor  integer,'
                   'start_time     timestamp,'
                   'duration       interval,'
                   'show_id        text,'
                   'episode_id     text,'
                   'rerun_code     character(1),'
                   'FOREIGN KEY (channel_major, channel_minor) REFERENCES {toSchema}.channel(major, minor),'
                   'FOREIGN KEY (show_id, episode_id) REFERENCES {toSchema}.episode(show_id, episode_id))').format(toSchema=toSchema)
        logger.info('Query: %s', query)
        cursor.execute(query)

        # Don't bother migrating the data in the 'schedule' table
        # It gets wiped and reinserted every time we fetch station listings


    dbConnection.commit()


