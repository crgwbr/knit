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
import simplejson
import threading
import sys
import Queue


class Server(object):
    SIG_STOP = 1
    SOCKET_TIMEOUT = 0.1
    PORT_RANGE = 1000
    
    token = None
    localAddress = None
    nodes = {}
    sock = None
    controlQueue = None
    
    def __init__(self, port, queuedConnections):
        self.sock = self.__getServerSocket(port, queuedConnections)
        self.controlQueue = Queue.Queue()
    
    
    def discoverMesh(self, remoteAddress):
        node = self.__addNode(remoteAddress)
        token, action, data = node.sendMessage("GetNodeList")
        
        for token, remoteAddress in data.iteritems():
            self.__addNode(remoteAddress)
        
        return self.nodes
    
    
    def doGetNodeList(self, clientNode, requestData):
        nodes = {}
        for token, node in self.nodes.iteritems():
            if token != clientNode.getToken():
                nodes[token] = node.getAddress()
        return nodes
    
    
    def doRegisterNewServer(self, clientNode, requestData):
        self.__addNode(requestData, clientNode.getToken())
    
    
    def getServerAddress(self):
        return self.localAddress
    
    
    def getServerToken(self):
        if not self.token:
            self.token = self.__generateServerToken()
        return self.token
    
    
    def listen(self):
        t = threading.Thread(target = self.__daemon)
        t.daemon = True
        t.start()
        return t
    
    
    def stop(self):
        self.controlQueue.put(self.SIG_STOP)
    
    
    def __addNode(self, remoteAddress, token = None):
        if token == self.getServerToken():
            return
        
        remoteAddress = tuple(remoteAddress)
        logging.info("Found New Node: %s:%s" % remoteAddress)
        
        node = Node(self, remoteAddress, token)
        self.nodes[node.token] = node
        return node
    
    
    def __daemon(self):
        while True:
            try:
                msg = self.controlQueue.get(False)
                if msg == self.SIG_STOP:
                    sys.exit()
            except Queue.Empty:
                pass
            
            self.__handleServerConnection()
    
    
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
        if clientToken in self.nodes.keys():
            return self.nodes[clientToken]
        
        return Node(self, remoteAddress, clientToken)


    def __getServerSocket(self, port, queuedConnections):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        while port <= (port + self.PORT_RANGE):
            try:
                self.localAddress = (socket.gethostname(), port)
                server.bind(self.localAddress)
                break
            except socket.error:
                port += 1
        
        server.settimeout(self.SOCKET_TIMEOUT)
        server.listen(queuedConnections)
        logging.info("Mesh Server listening on %s:%s" % self.localAddress)
        return server


    def __handleServerConnection(self):
        # Wait for connection
        try:
            client, remoteAddress = self.sock.accept()
        except socket.timeout:
            return
        
        logging.info("Incoming Connection from %s:%s" % remoteAddress)
        
        # Process the request
        messaging = MessagingSocket(client, self)
        clientToken, action, requestData = messaging.recv()
        
        # Find the client Node
        node = self.__getNode(clientToken, remoteAddress)
        
        # Perform the requested action
        fn = self.__resolveAction(action)
        responseData = fn(node, requestData) if fn else None
        
        # Send an ACKNOWLEDGEnowledgement along with the response data
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
    
    sock = None
    localServer = None
    
    def __init__(self, sock, localServer):
        self.sock = sock
        self.localServer = localServer
    
    
    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except socket.error:
            pass
    
    
    def send(self, action, data):
        message = self.__assembleMessage(action, data)
        
        totalsent = 0
        while totalsent < len(message):
            chunk = message[totalsent:]
            sent = self.sock.send(chunk)
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
                chunk = self.sock.recv(1024)
                response = response + chunk
            except socket.error:
                chunk = ''
            
            if chunk == '' or response.endswith(self.DELIMITER):
                break
        
        if not response.endswith(self.DELIMITER):
            raise RuntimeError("Socket connection to broken during receive.")
        
        return self.__disassembleMessage(response)
    
    
    def __assembleMessage(self, action, data = None):
        data = simplejson.dumps(data)
        parts = self.localServer.getServerToken(), action, data
        message = self.SEPERATOR.join(parts)
        return message + self.DELIMITER
    
    
    def __disassembleMessage(self, message):
        if not message.endswith(self.DELIMITER):
            raise RuntimeError("Attempted to decode malformed message.")
        
        message = message.rstrip(self.DELIMITER)
        senderToken, action, data = message.split(self.SEPERATOR)
        data = simplejson.loads(data)
        return senderToken, action, data


class Node(object):
    localServer = None
    address = None
    token = None
    
    def __init__(self, localServer, remoteAddress, token = None):
        self.localServer = localServer
        self.address = tuple(remoteAddress)
        
        if not token:
            token, action, data = self.__sendRegisterNewServer()
        
        self.token = token
    
    
    def getAddress(self):
        return self.address
    
    
    def getToken(self):
        return self.token
    
    
    def sendMessage(self, action, data = None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.address)
        
        messaging = MessagingSocket(s, self.localServer)
        messaging.send(action, data)
        response = messaging.recv()
        messaging.close()
        
        return response
    
    
    def __sendRegisterNewServer(self):
        return self.sendMessage("RegisterNewServer", self.localServer.getServerAddress())

