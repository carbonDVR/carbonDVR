#!/usr/bin/env python3.4

import argparse
import fetchXTVD
import logging
import os

if __name__ == '__main__':
    FORMAT = "%(asctime)-15s: %(name)s:  %(message)s"
    logging.basicConfig(level=logging.INFO, format=FORMAT)

    username = os.environ.get('SCHEDULES_DIRECT_USERNAME')
    password = os.environ.get('SCHEDULES_DIRECT_PASSWORD')

    username_required = (not username)
    password_required = (not password)

    parser = argparse.ArgumentParser(description='Fetch listings from SchedulesDirect in XTVD format.')
    parser.add_argument('-u', '--username', required=username_required)
    parser.add_argument('-p', '--password', required=password_required)
    parser.add_argument('-f', '--file', default='ddata.xml')
    args = parser.parse_args()

    if args.username:
        username = args.username
    if args.password:
        password = args.password
    
    fetchXTVD.fetchXTVDtoFile(username, password, filename=args.file)

