# -*- coding: utf-8 -*-
# 
# knit.proxy
# 
# Module containing components for proxying HTTP traffic

from wsgiref.simple_server import make_server
from wsgiref.util import is_hop_by_hop
import logging
import requests
import re


class WSGIEnviron(object):
    def __init__(self, data = {}):
        self.data = data
    
    def iteritems(self):
        for item in self.data.iteritems():
            yield item
    
    def __getitem__(self, key):
        return self.data.get(key, '')
    
    def __setitem__(self, key, value):
        self.data[key] = value


class HTTPProxyServer:
    frontend = None
    backend = None
    cache = None
    
    cacheMethods = []
    cacheRules = []
    preventCachingControls = ('private', 'no-cache', 'no-store', 'must-revalidate', 'proxy-revalidate')
    
    
    def __init__(self, frontend, backend, cache = None):
        self.frontend = frontend
        self.backend = backend
        self.cache = cache
        
        self.setCacheMethods(('GET', 'HEAD'))
        self.setCacheRules((("^.*$", "%(PATH_INFO)s;%(QUERY_STRING)s;%(HTTP_COOKIE)s"),))
    
    
    def run(self):
        logging.info("Running HTTP Proxy on %(host)s:%(port)s" % self.frontend)
        logging.info("Using HTTP backend %(host)s:%(port)s" % self.backend)
        
        httpd = make_server(self.frontend['host'], self.frontend['port'], self)
        httpd.serve_forever()
    
    
    def setCacheMethods(self, methods):
        if not methods:
            return
        self.cacheMethods = methods
    
    
    def setCacheRules(self, rules):
        if not rules:
            return
        
        self.cacheRules = []
        for pattern, format in rules:
            cre = re.compile(pattern)
            rule = cre, format
            self.cacheRules.append(rule)
    
    
    def __call__(self, environ, startResponse):
        self.environ = WSGIEnviron(environ)
        self.startResponse = startResponse
        return self
    
    
    def __iter__(self):
        url = self.__assembleBackendURL()
        cacheKey = self.__generateCacheKey(url)
        
        responseParts = None
        saveToCache = False
        if cacheKey:
            responseParts = self.cache.get(cacheKey)
        
        if not responseParts:
            responseParts = self.__fetchFromBackend(url)
            saveToCache = True
        
        body, status, responseHeaders = responseParts
        
        if cacheKey and saveToCache:
            timeout = self.__calculateCacheTimeout(dict(responseHeaders))
            if timeout > 0:
                self.cache.set(cacheKey, responseParts, timeout)
        
        self.startResponse(status, responseHeaders)
        yield body
    
    
    def __assembleBackendURL(self):
        url = "%s://%s:%s%s" % (
            self.environ['wsgi.url_scheme'],
            self.backend['host'],
            self.backend['port'],
            self.environ['PATH_INFO'])
        
        if self.environ['QUERY_STRING']:
            url += "?%s" % self.environ['QUERY_STRING']
        
        return url
    
    
    def __assembleRequestHeaders(self):
        headers = {}
        for key, value in self.environ.iteritems():
            if key.startswith('HTTP_'):
                key = key[5:].replace('_', '-').title()
                if not is_hop_by_hop(key):
                   headers[key] = value
        
        headers['Host'] = self.backend['host']
        return headers
    
    
    def __assembleResponseHeaders(self, headers):
        responseHeaders = []
        for key, value in headers.iteritems():
            if not is_hop_by_hop(key):
                key = key.title()
                responseHeaders.append((key, value))
        return responseHeaders
    
    
    def __calculateCacheTimeout(self, headers):
        cacheControl = headers.get('Cache-Control', '').split(',')
        cacheControl = [control.strip() for control in cacheControl]
        if 'public' not in cacheControl:
            return -1
        
        maxAge = -1
        for control in cacheControl:
            if control in self.preventCachingControls:
                return -1
            
            try:
                if "=" in control:
                    parts = control.split('=')
                    maxAge = int(parts[1])
            except (ValueError, IndexError):
                return -1
        
        return maxAge
    
    
    def __fetchFromBackend(self, url):
        fn = self.__getRequestHandler()
        headers = self.__assembleRequestHeaders()
        response = fn(url, headers=headers, allow_redirects=False, stream=True)
        responseHeaders = self.__assembleResponseHeaders(response.headers)
        status = "%s %s" % (response.status_code, requests.codes[response.status_code])
        return response.raw.read(), status, responseHeaders
    
    
    def __generateCacheKey(self, url):
        if self.environ['REQUEST_METHOD'] not in self.cacheMethods:
            return None
        
        for rule, keyFormat in self.cacheRules:
            if rule.match(url):
                return keyFormat % self.environ
        
        return None
    
    
    def __getRequestHandler(self):
        method = self.environ['REQUEST_METHOD'].lower()
        if hasattr(requests, method):
            fn = getattr(requests, method)
            if callable(fn):
                return fn
        return requests.get

