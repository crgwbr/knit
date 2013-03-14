# -*- coding: utf-8 -*-
# 
# knit.proxy
# 
# Module containing components for proxying HTTP traffic

from wsgiref.simple_server import make_server
from wsgiref.util import is_hop_by_hop
import logging
import requests


class HTTPProxyServer:
    frontend = None
    backend = None
    
    @classmethod
    def start(cls, frontend, backend):
        httpd = make_server(frontend['host'], frontend['port'], cls)
        logging.info("Running HTTP Proxy on %(host)s:%(port)s" % frontend)
        logging.info("Using HTTP backend %(host)s:%(port)s" % backend)
        
        cls.frontend = frontend
        cls.backend = backend
        
        # Respond to requests until process is killed
        httpd.serve_forever()
    
    
    def __init__(self, environ, startResponse):
        self.environ = environ
        self.startResponse = startResponse
    
    
    def __iter__(self):
        url = self.__assembleBackendURL()
        headers = self.__assembleRequestHeaders()
        fn = self.__getRequestHandler()
        logging.info("Proxied %s" % url)
        
        response = fn(url, headers=headers, allow_redirects=False, stream=True)
        
        responseHeaders = self.__assembleResponseHeaders(response.headers)
        status = "%s %s" % (response.status_code, requests.codes[response.status_code])
        self.startResponse(status, responseHeaders)
        
        yield response.raw.read()
    
    
    def __assembleBackendURL(self):
        url = "%s://%s:%s%s" % (
            self.environ['wsgi.url_scheme'],
            self.backend['host'],
            self.backend['port'],
            self.environ['PATH_INFO']
        )
        
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
        return headers
    
    
    def __assembleResponseHeaders(self, headers):
        responseHeaders = []
        for key, value in headers.iteritems():
            if not is_hop_by_hop(key):
                responseHeaders.append((key, value))
        return responseHeaders
    
    
    def __getRequestHandler(self):
        method = self.environ['REQUEST_METHOD'].lower()
        if hasattr(requests, method):
            fn = getattr(requests, method)
            if callable(fn):
                return fn
        return requests.get

