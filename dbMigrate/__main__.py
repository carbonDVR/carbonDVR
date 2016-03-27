#!/usr/bin/env python3.4

import argparse
import sys, os, os.path
import logging
import psycopg2

import dbMigrate

if __name__ == '__main__':
    FORMAT = "%(asctime)-15s: %(name)s:  %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Migrate database from one version of CarbonDVR to another.')
    parser.add_argument('--from-version', required=True, dest='fromVersion', choices=['1.0'])
    parser.add_argument('--to-version', required=True, dest='toVersion', choices=['2.0'])
    parser.add_argument('--from-database', required=True, dest='fromDatabase')
    parser.add_argument('--to-database', required=True, dest='toDatabase')
    parser.add_argument('--from-schema', required=True, dest='fromSchema')
    parser.add_argument('--to-schema', required=True, dest='toSchema')
    args = parser.parse_args()

    fromDB = psycopg2.connect(args.fromDatabase)
    toDB = psycopg2.connect(args.toDatabase)

    if args.fromDatabase == args.toDatabase and args.fromSchema == args.toSchema:
        logger.error('Cannot specify the same database and schema as both "from" and "to".')
        sys.exit(1)

    with fromDB:
        with fromDB.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (args.fromSchema, ))

    with toDB:
        with toDB.cursor() as cursor:
            cursor.execute("SET SCHEMA %s", (args.toSchema, ))

    if args.fromVersion == '1.0' and args.toVersion == '2.0':
        logger.info('Migrating from version "%s" in schema "%s" to version "%s" in schema "%s"', args.fromVersion, args.fromSchema, args.toVersion, args.toSchema)
        dbMigrate.migrate1to2(fromDB, args.fromSchema, toDB, args.toSchema)
    else:
        logger.error('Migration from version "%s" to version "%s" is not supported.', args.fromVersion, args.toVersion)
        sys.exit(1)

