#!/usr/bin/env python3.4

import argparse
import sys, os, os.path
import logging
import psycopg2

import dbMigrate

def getMandatoryEnvVar(varName):
    logger = logging.getLogger(__name__)
    value = os.environ.get(varName)
    if value is None:
        logger.error('%s environment variable is not set', envVar)
        sys.exit(1)
    logger.info('%s=%s', varName, value)
    return value

def schemaExists(dbConnection, schemaName):
    with dbConnection.cursor() as cursor:
        cursor.execute('SELECT COUNT(schema_name) FROM information_schema.schemata WHERE schema_name = %s', (schemaName,))
        return cursor.fetchone()[0] > 0



if __name__ == '__main__':
    FORMAT = "%(asctime)-15s: %(name)s:  %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(__name__)

    dbConnectString = getMandatoryEnvVar('DB_CONNECT_STRING')

    parser = argparse.ArgumentParser(description='Migrate database from one version of CarbonDVR to another.')
    parser.add_argument('--from-version', required=True, dest='fromVersion', choices=['1.0'])
    parser.add_argument('--to-version', required=True, dest='toVersion', choices=['2.0'])
    parser.add_argument('--from-schema', required=True, dest='fromSchema')
    parser.add_argument('--to-schema', required=True, dest='toSchema')
    parser.add_argument('--replace-existing', required=False, dest='replaceExisting', action='store_true')
    args = parser.parse_args()

    dbConnection = psycopg2.connect(dbConnectString)

    if args.fromSchema == args.toSchema:
        logger.error('Cannot specify the same schema as both --from-schema and --to-schema.')
        sys.exit(1)

    if not schemaExists(dbConnection, args.fromSchema):
        logger.error('Schema "%s" does not exist.  Schema passed in --from-schema must exist.', args.fromSchema)
        sys.exit(1)
        
    if schemaExists(dbConnection, args.toSchema) and not args.replaceExisting:
        logger.error('Schema "%s" already exists and --replace-existing was not specified.', args.toSchema)
        sys.exit(1)

    if args.fromVersion == '1.0' and args.toVersion == '2.0':
        logger.info('Migrating from version "%s" in schema "%s" to version "%s" in schema "%s"', args.fromVersion, args.fromSchema, args.toVersion, args.toSchema)
        dbMigrate.migrate1to2(dbConnection=dbConnection, fromSchema=args.fromSchema, toSchema=args.toSchema, replaceToSchema=args.replaceExisting)
    else:
        logger.error('Migration from version "%s" to version "%s" is not supported.', args.fromVersion, args.toVersion)
        sys.exit(1)

