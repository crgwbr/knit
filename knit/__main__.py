#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 
# knit.__main__

from optparse import OptionParser
from wsgiref.simple_server import make_server
import os
import yaml
import logging
import sys
import signal

from mesh import (
    MeshCache,
    MeshServer
)

from proxy import (
    HTTPProxyServer
)


class KnitMeshProxy(object):
    settings = {}
    options = None
    meshServer = None
    meshThread = None
    
    def __init__(self):
        self.__setupLogging()
        self.__buildMeshServer()
    
    
    def getWSGIApplication(self):
        self.__discoverMeshNetwork()
        self.__startMeshServer()
        return self.__initProxyServer()
    
    
    def isDevelopmentMode(self):
        return self.__getEnvironmentSetting('devel')
    
    
    def startDevelopmentServer(self, application):
        self.__setupErrorHandling()
        httpFrontend = self.__getConfigSetting('http.frontend')
        logging.info("Running HTTP Development Server on %(host)s:%(port)s" % httpFrontend)
        httpd = make_server(httpFrontend['host'], httpFrontend['port'], application)
        httpd.serve_forever()
    
    
    def __buildMeshServer(self):
        port = self.__getConfigSetting('mesh.port')
        queuedConnections = self.__getConfigSetting('mesh.queue')
        self.meshServer = MeshServer(port, queuedConnections)
    
    
    def __discoverMeshNetwork(self):
        discover = self.__getEnvironmentSetting('discover')
        if not discover:
            return
        
        host, port = discover.split(":")
        remoteAddress = host, int(port)
        self.meshServer.discoverMesh(remoteAddress)
    
    
    def __getConfigSetting(self, key):
        defaults = self.__openSettingsFile(self.__getDefaultSettingsPath())
        
        settingsPath = self.__getEnvironmentSetting('settings')
        settings = self.__openSettingsFile(settingsPath)
        
        subDefault = defaults
        subSetting = settings
        for segment in key.split('.'):
            subDefault = subDefault or {}
            subDefault = subDefault.get(segment)
            subSetting = subSetting or {}
            subSetting = subSetting.get(segment)
        
        return subSetting or subDefault
    
    
    def __getDefaultSettingsPath(self, name = "default.yml"):
        path = os.path.abspath(__file__)
        path = path.split(os.sep)
        path.pop()
        path.append(name)
        path = os.sep.join(path)
        return path
    
    
    def __getEnvironmentSetting(self, key):
        options = self.__loadEnvironmentSettings()
        if not hasattr(options, key):
            return None
        
        envKey = "KNIT_%s" % key.upper()
        value = getattr(options, key) or os.environ.get(envKey)
        
        if value == "True": value = True
        if value == "False": value = False
        
        return value
    
    
    def __initProxyServer(self):
        cacheBackend = self.__getConfigSetting('cache.backend')
        cache = MeshCache(self.meshServer, cacheBackend)
        self.meshServer.setCacheBackend(cache)
        
        httpBackend = self.__getConfigSetting('http.backend')
        logging.info("Using HTTP backend %(host)s:%(port)s" % httpBackend)
        
        server = HTTPProxyServer(httpBackend, cache = cache)
        server.setCacheMethods(self.__getConfigSetting('cache.methods'))
        server.setCacheRules(self.__getConfigSetting('cache.rules'))
        return server
    
    
    def __loadEnvironmentSettings(self):
        if self.options:
            return self.options
        
        parser = OptionParser()
        
        parser.add_option("-s", "--settings", 
            help="Custom Settings File", 
            metavar="FILE")
        
        parser.add_option("-d", "--discover", 
            help="Mesh Discovery Address", 
            metavar="HOST:PORT")
        
        parser.add_option("--devel", 
            help="Run HTTP Development Server. Production environments should use a real WSGI Server instead.", 
            action="store_true")
        
        options, args = parser.parse_args()
        self.options = options
        return options
    
    
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
            logging.critical("Caught signal %s." % signum)
            logging.critical("Waiting for threads to exit.")
            self.meshServer.stop()
            self.meshThread.join()
            logging.critical("Main thread exiting now.")
            sys.exit()
        
        signal.signal(signal.SIGINT, die)
        signal.signal(signal.SIGTSTP, die)
    
    
    def __setupLogging(self):
        logFormat = self.__getConfigSetting('log.format')
        
        logLevel = self.__getConfigSetting('log.level')
        logLevel = getattr(logging, logLevel) if hasattr(logging, logLevel) else None
        
        stream = self.__getConfigSetting('log.stream')
        stream, filename = (getattr(sys, stream), None) if hasattr(sys, stream) else (None, stream)
        
        logging.basicConfig(level=logLevel, format=logFormat, stream=stream, filename=filename)
    
    
    def __startMeshServer(self):
        self.meshThread = self.meshServer.listen()


proxy = KnitMeshProxy()
application = proxy.getWSGIApplication()

if proxy.isDevelopmentMode():
    proxy.startDevelopmentServer(application)
