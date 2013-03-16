# -*- coding: utf-8 -*-
# 
# knit.mesh
# 
# Module containing components for building and comunicating
# over a mesh network.

import hashlib
import time
import random
import socket
import logging
import yaml
import base64
import threading
import sys
import Queue
from simplecache import Cache


class MeshCache(Cache):
    def __init__(self, meshServer, backend, **config):
        self.meshServer = meshServer
        Cache.__init__(self, backend, **config)
    
    def set(self, key, value, expire = 0, replicate = True):
        if replicate:
            self.meshServer.replicateCacheEntry(key, value, expire)
        return Cache.set(self, key, value, expire)



class MeshServer(object):
    SIG_STOP = 1
    SOCKET_TIMEOUT = 1
    PORT_RANGE = 1000
    
    __token = None
    __localAddress = None
    __nodes = {}
    __sock = None
    __controlQueue = None
    __cacheBackend = None
    
    def __init__(self, port, queuedConnections):
        self.__sock = self.__getServerSocket(port, queuedConnections)
        self.__controlQueue = Queue.Queue()
    
    
    def discoverMesh(self, remoteAddress):
        node = self.__addNode(remoteAddress)
        token, action, data = node.sendMessage("GetNodeList")
        
        for token, remoteAddress in data.iteritems():
            self.__addNode(remoteAddress)
        
        return self.__nodes
    
    
    def doGetNodeList(self, clientNode, requestData):
        nodes = {}
        for token, node in self.__nodes.iteritems():
            if token != clientNode.getToken():
                nodes[token] = node.getAddress()
        return nodes
    
    
    def doRegisterNewServer(self, clientNode, requestData):
        self.__addNode(requestData, clientNode.getToken())
    
    
    def doSaveCacheEntry(self, clientNode, requestData):
        key, value, expire = requestData
        logging.debug("Cache entry push from %s for key %s" % (clientNode.getToken(), key))
        self.__cacheBackend.set(key, value, expire, replicate = False)
    
    
    def getServerAddress(self):
        return self.__localAddress
    
    
    def getServerToken(self):
        if not self.__token:
            self.__token = self.__generateServerToken()
        return self.__token
    
    
    def listen(self):
        t = threading.Thread(target = self.__daemon)
        t.daemon = True
        t.start()
        return t
    
    
    def replicateCacheEntry(self, key, value, expire):
        self.__broadcast('SaveCacheEntry', (key, value, expire))
    
    
    def setCacheBackend(self, backend):
        self.__cacheBackend = backend
    
    
    def stop(self):
        logging.critical("Sending halt signal to mesh server.")
        self.__controlQueue.put(self.SIG_STOP)
    
    
    def __addNode(self, remoteAddress, token = None):
        if token == self.getServerToken():
            return
        
        remoteAddress = tuple(remoteAddress)
        logging.info("Found New Node: %s:%s" % remoteAddress)
        
        node = Node(self, remoteAddress, token)
        self.__nodes[node.getToken()] = node
        return node
    
    
    def __broadcast(self, action, data):
        def daemon():
            for token, node in self.__nodes.iteritems():
                recvToken, recvAction, recvData = node.sendMessage(action, data)
                if recvAction != MessagingSocket.ACKNOWLEDGE:
                    logging.error("Failed to receive acknowledgement from %s/%s" % (node.getToken(), action))
        t = threading.Thread(target = daemon)
        t.daemon = True
        t.start()
    
    
    def __daemon(self):
        while True:
            try:
                msg = self.__controlQueue.get(False)
                if msg == self.SIG_STOP:
                    logging.critical("Mesh server exiting now.")
                    sys.exit()
            except Queue.Empty:
                pass
            
            try:
                self.__handleServerConnection()
            except Exception, e:
                logging.exception("Caught Exception: %s" % e)
    
    
    def __generateServerToken(self):
        stamp = time.time()
        random.seed(stamp)
        
        rand = 100
        for i in range(10):
            rand = rand * random.random()
        rand = rand * 1000000
        
        token = "%s-%s-%s" % (stamp, rand, socket.gethostname())
        return hashlib.md5(token).hexdigest()
    
    
    def __getNode(self, clientToken, remoteAddress):
        if clientToken in self.__nodes.keys():
            return self.__nodes[clientToken]
        
        return Node(self, remoteAddress, clientToken)


    def __getServerSocket(self, port, queuedConnections):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        while port <= (port + self.PORT_RANGE):
            try:
                self.__localAddress = (socket.gethostname(), port)
                server.bind(self.__localAddress)
                break
            except socket.error:
                port += 1
        
        server.settimeout(self.SOCKET_TIMEOUT)
        server.listen(queuedConnections)
        logging.info("Mesh Server listening on %s:%s" % self.__localAddress)
        return server


    def __handleServerConnection(self):
        # Wait for connection
        try:
            client, remoteAddress = self.__sock.accept()
        except socket.timeout:
            return
        
        logging.debug("Incoming Connection from %s:%s" % remoteAddress)
        
        # Process the request
        messaging = MessagingSocket(client, self)
        clientToken, action, requestData = messaging.recv()
        
        # Find the client Node
        node = self.__getNode(clientToken, remoteAddress)
        
        # Perform the requested action
        fn = self.__resolveAction(action)
        responseData = fn(node, requestData) if fn else None
        
        # Send an Acknowledgement along with the response data
        messaging.sendAck(responseData)
        messaging.close()
    
    
    def __resolveAction(self, action):
        fnName = "do%s" % action
        if hasattr(self, fnName):
            fn = getattr(self, fnName)
            if callable(fn):
                return fn


class MessagingSocket(object):
    SEPERATOR = "&&"
    DELIMITER = ";;"
    ACKNOWLEDGE = "Ok."
    
    __sock = None
    __localServer = None
    
    def __init__(self, sock, localServer):
        self.__sock = sock
        self.__localServer = localServer
    
    
    def close(self):
        try:
            self.__sock.shutdown(socket.SHUT_RDWR)
            self.__sock.close()
        except socket.error:
            pass
    
    
    def send(self, action, data):
        message = self.__assembleMessage(action, data)
        
        totalsent = 0
        while totalsent < len(message):
            chunk = message[totalsent:]
            sent = self.__sock.send(chunk)
            if sent == 0:
                raise RuntimeError("Socket connection broken during send.")
            totalsent = totalsent + sent
        
        return totalsent
    
    
    def sendAck(self, data):
        return self.send(self.ACKNOWLEDGE, data)
    
    
    def recv(self):
        response = ''
        
        while True:
            try:
                chunk = self.__sock.recv(1024)
                response = response + chunk
            except socket.error, e:
                chunk = ''
                # UGLY Hack around BSD platforms raising errors when sockets are
                # temp unavailable. TODO: Make this prettier
                if str(e) == "[Errno 35] Resource temporarily unavailable":
                    time.sleep(0)
                    continue
            
            if chunk == '' or response.endswith(self.DELIMITER):
                break
        
        return self.__disassembleMessage(response)
    
    
    def __assembleMessage(self, action, data = None):
        data = yaml.dump(data)
        parts = self.__localServer.getServerToken(), action, data
        message = self.SEPERATOR.join(parts)
        message = base64.b64encode(message)
        message = message + self.DELIMITER
        return message
    
    
    def __disassembleMessage(self, message):
        if not message.endswith(self.DELIMITER):
            raise RuntimeError("Attempted to decode malformed message: %s" % message)
        
        message = message[:-len(self.DELIMITER)]
        message = base64.b64decode(message)
        senderToken, action, data = message.split(self.SEPERATOR)
        data = yaml.load(data)
        return senderToken, action, data


class Node(object):
    __localServer = None
    __address = None
    __token = None
    
    def __init__(self, localServer, remoteAddress, token = None):
        self.__localServer = localServer
        self.__address = tuple(remoteAddress)
        
        if not token:
            token, action, data = self.__sendRegisterNewServer()
        
        self.__token = token
    
    
    def getAddress(self):
        return self.__address
    
    
    def getToken(self):
        return self.__token
    
    
    def sendMessage(self, action, data = None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.__address)
        
        messaging = MessagingSocket(s, self.__localServer)
        messaging.send(action, data)
        response = messaging.recv()
        messaging.close()
        
        return response
    
    
    def __sendRegisterNewServer(self):
        return self.sendMessage("RegisterNewServer", self.__localServer.getServerAddress())

