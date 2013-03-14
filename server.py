#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
from optparse import OptionParser
import logging
import sys
import signal
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
    queuedConnections = getSetting('mesh.queue', options.settings)
    meshServer = knit.Server(port, queuedConnections)
    
    if options.discover:
        host, port = options.discover.split(":")
        remoteAddress = host, int(port)
        meshServer.discoverMesh(remoteAddress)
    
    thread = meshServer.listen()
    
    def die(signum, frame):
        logging.info("Caught Signal %s." % signum)
        meshServer.stop()
        thread.join()
        sys.exit()
    
    signal.signal(signal.SIGINT, die)
    signal.signal(signal.SIGTSTP, die)
    
    frontend = getSetting('frontend', options.settings)
    backend = getSetting('backend', options.settings)
    knit.HTTPProxyServer.start(frontend, backend)
    

if __name__ == "__main__":
    main()