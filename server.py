#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
from optparse import OptionParser
import logging
import sys
import signal
import knit


class KnitMeshProxy(object):
    settings = {}
    options = None
    meshServer = None
    meshThread = None
    
    def __init__(self):
        self.__loadOptions()
        self.__setupLogging()
        self.__buildMeshServer()
        self.__discoverMeshNetwork()
    
    
    def __call__(self):
        self.__setupErrorHandling()
        self.__startMeshServer()
        self.__startProxyServer()
    
    
    def __buildMeshServer(self):
        port = self.__getSetting('mesh.port')
        queuedConnections = self.__getSetting('mesh.queue')
        self.meshServer = knit.MeshServer(port, queuedConnections)
    
    
    def __discoverMeshNetwork(self):
        if not self.options.discover:
            return
        
        host, port = self.options.discover.split(":")
        remoteAddress = host, int(port)
        self.meshServer.discoverMesh(remoteAddress)
    
    
    def __getSetting(self, key):
        defaults = self.__openSettingsFile('default.yml')
        settings = self.__openSettingsFile(self.options.settings)
        
        subDefault = defaults
        subSetting = settings
        for segment in key.split('.'):
            subDefault = subDefault or {}
            subDefault = subDefault.get(segment)
            subSetting = subSetting or {}
            subSetting = subSetting.get(segment)
        
        return subSetting or subDefault
    
    
    def __loadOptions(self):
        parser = OptionParser()
        parser.add_option("-s", "--settings", help="Custom Settings File", metavar="FILE")
        parser.add_option("-d", "--discover", help="Mesh Discovery Address", metavar="HOST:PORT")
        options, args = parser.parse_args()
        self.options = options
    
    
    def __openSettingsFile(self, path):
        if not path:
            return {}
        
        if path in self.settings.keys():
            return self.settings[path]
        
        try:
            defaultsFile = open(path, 'rU')
        except IOError:
            return {}
        
        settings = yaml.load(defaultsFile)
        self.settings[path] = settings
        return settings
    
    
    def __setupErrorHandling(self):
        def die(signum, frame):
            logging.info("Caught Signal %s." % signum)
            self.meshServer.stop()
            self.meshThread.join()
            sys.exit()
        
        signal.signal(signal.SIGINT, die)
        signal.signal(signal.SIGTSTP, die)
    
    
    def __setupLogging(self):
        logFormat = self.__getSetting('log.format')
        logLevel = self.__getSetting('log.level')
        logLevel = getattr(logging, logLevel) if hasattr(logging, logLevel) else logging.DEBUG
        
        logging.basicConfig(level=logLevel, format=logFormat)
    
    
    def __startMeshServer(self):
        self.meshThread = self.meshServer.listen()
    
    
    def __startProxyServer(self):
        cacheBackend = self.__getSetting('cache.backend')
        cache = knit.MeshCache(self.meshServer, cacheBackend)
        
        httpFrontend = self.__getSetting('http.frontend')
        httpBackend = self.__getSetting('http.backend')
        
        server = knit.HTTPProxyServer(httpFrontend, httpBackend, cache = cache)
        server.setCacheMethods(self.__getSetting('cache.methods'))
        server.setCacheRules(self.__getSetting('cache.rules'))
        server.run()
    

if __name__ == "__main__":
    proxy = KnitMeshProxy()
    proxy()