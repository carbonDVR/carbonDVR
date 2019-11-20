#!/usr/bin/env python3.4

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import string
import sys
import time

import codecs
import encodings
import gzip
import urllib.request
import urllib.parse
import requests
from requests.auth import HTTPBasicAuth


# fetchXTVD is adapted from code released into the Public Domain by Keith Medcalf
# original license statement follows:
##
## This module retrieves SchedulesDirect XML data using a hand-coded SOAP request.
##
## The code is released into the Public Domain.  If you break it, you own both halves.
##
## Original Code by Keith Medcalf, kmedcalf@dessus.com
## posted on 2008-09-13 http://forums.schedulesdirect.org/viewtopic.php?f=8&t=595
def fetchXTVD_original(userName,
             passWord,
             URL='http://dd.schedulesdirect.org/schedulesdirect/tvlistings/xtvdService',
             Realm='TMSWebServiceRealm',
             predays=0,
             postdays=14,
             fileName='ddata.xml',
             fileCoding='latin-1',
             gzipped = False):
    strSoap = '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n' \
              ' <SOAP-ENV:Body>\n' \
              '  <m:download xmlns:m="urn:TMSWebServices" SOAP-ENV:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">\n' \
              '   <startTime xsi:type="xsd:dateTime">' + startTime + '</startTime>\n' \
              '   <endTime xsi:type="xsd:dateTime">' + endTime + '</endTime>\n' \
              '  </m:download>\n' \
              ' </SOAP-ENV:Body>\n' \
              '</SOAP-ENV:Envelope>'
    print('#', time.strftime('%Y/%m/%d %H:%M:%S'), "Retrieving DataDirect TV Schedules")
    print('#', time.strftime('%Y/%m/%d %H:%M:%S'), "Requesting", startTime, "to", endTime)
    authinfo = urllib.request.HTTPBasicAuthHandler()
    authinfo.add_password(Realm, urllib.parse.urlparse(URL)[1], userName, passWord)
    request = urllib.request.Request(URL, strSoap.encode('ascii'))
    if gzipped:
        request.add_header('Accept-encoding', 'gzip')
        if fileName[-3:].lower() == '.gz':
            fileName = fileName[:-3]
        fileName += '.gz'
    opener = urllib.request.build_opener(authinfo)
    urllib.request.install_opener(opener)
    print('#', time.strftime('%Y/%m/%d %H:%M:%S'), 'Saving XML to File: ' + fileName + ', Encoding: ' + fileCoding)
    fileObj = None
    if fileCoding == 'native':
        urldata = opener.open(request)
        outfile = open(fileName,'wb',262144)
        repenc = False
    elif not gzipped:
        urldata = codecs.getreader('utf-8')(opener.open(request), errors='replace')
        outfile = codecs.open(fileName,'wb', fileCoding, 'replace', 262144)
        repenc = True
    else:
        raise ValueError('Codepage Translation of GZIP data not supported')
    print('#', time.strftime('%Y/%m/%d %H:%M:%S'), 'Receiving XML Data', ' '*30,)
    fmt = ('\b'*30) + '%6d KB, %3d KB/s, %3d KB/s'
    data = 'X'
    bytes = 0
    currb = 0
    first = time.time()
    last = time.time() - 1
    while data:
        data = urldata.read(8192)
        b = len(data)
        bytes += b
        currb += b
        if repenc:
            data = str.replace(data, "encoding='utf-8'", "encoding='"+fileCoding+"'")
            repenc = False
        if data:
            outfile.write(data)
        curr = time.time()
        diff = curr - last
        if diff >= 0.999:
            print( fmt % ((bytes//1024), currb//1024//(curr-last), bytes//1024//(curr-first)),)
            last = curr
            currb = 0
    urldata.close()
    outfile.close()
    if fileObj:
        fileObj.close()
    print( fmt % ((bytes//1024), 0, bytes//1024//(curr-first)))
    print('#', time.strftime('%Y/%m/%d %H:%M:%S'), "Data Retrieval Complete")


def buildXTVDSoapRequest(startDatetime, endDatetime):
    timeFormat = '%Y-%m-%dT00:00:00Z'
    startTime = startDatetime.strftime(timeFormat)
    endTime   = endDatetime.strftime(timeFormat)
    strSoap = '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:SOAP-ENC="http://schemas.xmlsoap.org/soap/encoding/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">\n' \
              ' <SOAP-ENV:Body>\n' \
              '  <m:download xmlns:m="urn:TMSWebServices" SOAP-ENV:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">\n' \
              '   <startTime xsi:type="xsd:dateTime">' + startTime + '</startTime>\n' \
              '   <endTime xsi:type="xsd:dateTime">' + endTime + '</endTime>\n' \
              '  </m:download>\n' \
              ' </SOAP-ENV:Body>\n' \
              '</SOAP-ENV:Envelope>'
    return strSoap


def fetchXTVD(username,
             password,
             startDatetime,
             endDatetime,
             URL='http://dd.schedulesdirect.org/schedulesdirect/tvlistings/xtvdService'):
    soapRequest = buildXTVDSoapRequest(startDatetime, endDatetime)
    # do we need to pass in an accept-encoding header for gzip? # headers = { 'Accept-encoding' : 'gzip'}
    response = requests.put(URL, data=soapRequest, auth=HTTPBasicAuth(username,password), stream=True)
    for data in response.iter_content(8192):
        yield data


def fetchXTVDtoFile(username,
             password,
             filename,
             predays=0,
             postdays=14):
    logger = logging.getLogger(__name__)
    currentTime = datetime.now(timezone.utc)
    startDatetime = currentTime + timedelta(days=predays)
    endDatetime = currentTime + timedelta(days=postdays)
    logger.info('Retrieving DataDirect TV schedules')
    with open(filename,'wb') as outfile:
        for chunk in fetchXTVD(username, password, startDatetime, endDatetime):
            outfile.write(chunk)
    logger.info('Retrieval complete')

