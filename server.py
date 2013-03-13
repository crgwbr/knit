#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
from optparse import OptionParser
import logging

import knit


def openSettingsFile(path):
    try:
        defaultsFile = open(path, 'rU')
    except IOError:
        return {}
    
    return yaml.load(defaultsFile)


def getSetting(key, settingsFile = None):
    defaults = openSettingsFile('default.yml')
    settings = openSettingsFile(settingsFile) if settingsFile else {}
    
    subDefault = defaults
    subSetting = settings
    for segment in key.split('.'):
        subDefault = subDefault or {}
        subDefault = subDefault.get(segment)
        subSetting = subSetting or {}
        subSetting = subSetting.get(segment)
    
    return subSetting or subDefault


def main():
    parser = OptionParser()
    parser.add_option("-s", "--settings", help="Custom Settings File", metavar="FILE")
    parser.add_option("-d", "--discover", help="Mesh Discovery Address", metavar="HOST:PORT")
    options, args = parser.parse_args()
    
    logFormat = getSetting('log_format', options.settings)
    logging.basicConfig(level=logging.DEBUG, format=logFormat)
    
    port = getSetting('mesh.port', options.settings)
    queue = getSetting('mesh.queue', options.settings)
    server = knit.getServerSocket(port, queue)
    
    if options.discover:
        host, port = options.discover.split(":")
        port = int(port)
        remoteAddress = (host, port)
        knit.discoverMesh(remoteAddress)
    
    knit.runMeshServer(server)


if __name__ == "__main__":
    main()