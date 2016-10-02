#!/usr/bin/env python3.4

import json

class FileLocations:
    def __init__(self, locationString):
        fileLocationsJson = json.loads(locationString)
        if 'fileLocations' in fileLocationsJson:
            self.fileLocations = fileLocationsJson['fileLocations']

    def getRawVideoFilespec(self, locationID, recordingID):
        try:
            for location in self.fileLocations['rawVideo']:
                if location['id'] == locationID:
                    return location['filespec'].format(recordingID = recordingID)
            return ''
        except:
            return ''

    def getTranscodedVideoFilespec(self, locationID, recordingID):
        try:
            for location in self.fileLocations['transcodedVideo']:
                if location['id'] == locationID:
                    return location['filespec'].format(recordingID = recordingID)
            return ''
        except:
            return ''

    def getTranscodedVideoURL(self, locationID, recordingID):
        try:
            for location in self.fileLocations['transcodedVideo']:
                if location['id'] == locationID:
                    return location['url'].format(recordingID = recordingID)
            return ''
        except:
            return ''

    def getBifFilespec(self, locationID, recordingID):
        try:
            for location in self.fileLocations['bif']:
                if location['id'] == locationID:
                    return location['filespec'].format(recordingID = recordingID)
            return ''
        except:
            return ''

    def getBifURL(self, locationID, recordingID):
        try:
            for location in self.fileLocations['bif']:
                if location['id'] == locationID:
                    return location['url'].format(recordingID = recordingID)
            return ''
        except:
            return ''


