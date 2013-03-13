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


token = None
localAddress = None
nodes = {}


def addNode(remoteAddress, token = None):
    global nodes
    logging.info("Found New Node: %s:%s" % tuple(remoteAddress))
    node = Node(remoteAddress, token)
    nodes[node.token] = node
    return node


def getNodeUniqueToken():
    global token
    if not token:
        token = generateNodeUniqueToken()
    return token


def generateNodeUniqueToken():
    stamp = time.time()
    random.seed(stamp)
    
    rand = 100
    for i in range(10):
        rand = rand * random.random()
    rand = rand * 1000000
    
    token = "%s-%s-%s" % (stamp, rand, socket.gethostname())
    return hashlib.md5(token).hexdigest()


def discoverMesh(remoteAddress):
    global nodes
    
    node = addNode(remoteAddress)
    token, action, data = node.sendMessage("GetNodeList")
    
    for token, remoteAddress in data.iteritems():
        addNode(remoteAddress)


def getServerSocket(port, queue):
    global localAddress
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    while True:
        try:
            localAddress = (socket.gethostname(), port)
            server.bind(localAddress)
            break
        except socket.error:
            port += 1
    
    server.listen(queue)
    logging.info("Mesh Server listening on %s:%s" % localAddress)
    return server


def handleServerConnection(server):
    global nodes
    
    (client, remoteAddress) = server.accept()
    logging.info("Incoming Connection from %s:%s" % remoteAddress)
    
    message = Node.recv(client)
    clientToken, action, requestData = Node.disassembleMessage(message)
    
    if clientToken in nodes.keys():
        node = nodes[clientToken]
    else:
        node = Node(remoteAddress, clientToken)
    
    fnName = "do%s" % action
    responseData = None
    if hasattr(node, fnName):
        fn = getattr(node, fnName)
        if callable(fn):
            responseData = fn(requestData)
    
    message = Node.assembleMessage(Node.ack, responseData)
    Node.send(client, message)
    
    try:
        client.shutdown(socket.SHUT_RDWR)
        client.close()
    except socket.error:
        pass


def runMeshServer(server):
    while True:
        handleServerConnection(server)


class Node(object):
    address = None
    token = None
    sep = "&&"
    delim = ";;"
    ack = "Ok."
    
    
    @classmethod
    def assembleMessage(cls, action, data = None):
        data = simplejson.dumps(data)
        parts = (getNodeUniqueToken(), action, data)
        message = cls.sep.join(parts)
        return message + cls.delim
    
    
    @classmethod
    def disassembleMessage(cls, message):
        if not message.endswith(cls.delim):
            raise RuntimeError("Attempted to decode malformed message.")
        message = message.rstrip(cls.delim)
        senderToken, action, data = message.split(cls.sep)
        data = simplejson.loads(data)
        return senderToken, action, data
    
    
    @classmethod
    def send(cls, sock, message):
        totalsent = 0
        while totalsent < len(message):
            chunk = message[totalsent:]
            sent = sock.send(chunk)
            if sent == 0:
                raise RuntimeError("Socket connection broken during send.")
            totalsent = totalsent + sent
        return totalsent
    
    
    @classmethod
    def recv(cls, sock):
        response = ''
        while True:
            try:
                chunk = sock.recv(1024)
                response = response + chunk
            except socket.error:
                chunk = ''
            if chunk == '' or response.endswith(cls.delim):
                break
        
        if not response.endswith(cls.delim):
            raise RuntimeError("Socket connection to broken during receive.")
        
        return response
    
    def __init__(self, remoteAddress, token = None):
        self.address = tuple(remoteAddress)
        
        if not token:
            token, action, data = self.sendRegisterNewServer()
        self.token = token
    
    
    def sendMessage(self, action, data = None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(self.address)
        
        message = self.assembleMessage(action, data)
        self.send(s, message)
        response = self.recv(s)
        response = self.disassembleMessage(response)
        
        try:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except socket.error:
            pass
        return response
    
    
    def sendRegisterNewServer(self):
        global localAddress
        return self.sendMessage("RegisterNewServer", localAddress)
    
    
    def doGetNodeList(self, requestData):
        global nodes
        nodeInfo = {}
        for token, node in nodes.iteritems():
            if token != self.token:
                nodeInfo[token] = node.address
        return nodeInfo
    
    
    def doRegisterNewServer(self, remoteAddress):
        addNode(remoteAddress, self.token)

