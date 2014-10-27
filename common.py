# -*- coding: iso-8859-1 -*-

import socket
import json
import xmltodict

    
# ==================== Classes ====================
class NetworkHandler():  
    def __init__(self, socketObject=None):
        if (socketObject): self.sock = socketObject
        else: self.sock = socket.socket()
        self.bufsize = 8192
        self.headersize = 15
        self.msgsize = self.bufsize - self.headersize - 1
        
    def connect(self, address, port):
        self.sock.connect((address, port))
        
    def getaddress(self):
        #return (socket.gethostbyaddr(self.sock.getpeername()[0])[0], self.sock.getpeername()[1])
        return self.sock.getpeername()
        
    def send(self, message):
        strMsg = json.dumps(message)
        splitMsg = [strMsg[i:i+self.msgsize] for i in range(0, len(strMsg), self.msgsize)]
        
        # Send intermediary packets
        header = json.dumps({"last": False})
        for i in range(len(splitMsg) - 1):
            packet = " ".join((header, splitMsg[i]))
            self.sock.sendall(packet)
        
        # Send final packet
        header = json.dumps({"last": True})
        packet = " ".join((header, splitMsg[-1]))
        self.sock.sendall(packet)
        
    def recv(self):
        strMsg = ""
        while (True):
            packet = self.sock.recv(self.bufsize)
            if not packet: return None
            header = json.loads(packet[:self.headersize])
            splitMsg = packet[self.headersize:]
            strMsg = "".join((strMsg, splitMsg))
            if header["last"]: break
        return json.loads(strMsg)    
    
    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        
        
# ==================== Methods ====================
def str2bool(stringToConvert):
    if stringToConvert.lower() in ("true", "t", "yes", "y", "on", "1"): return True
    if stringToConvert.lower() in ("false", "f", "no", "n", "off", "0"): return False
    raise TypeError("The value '%s' is not considered a valid boolean in this context." % stringToConvert)
    
def loadConfig(configFilePath):
    configFile = open(configFilePath, "r")
    configDict = xmltodict.parse(configFile.read())
    config = configDict["config"]

    # Type conversions
    config["global"]["connection"]["port"] = int(config["global"]["connection"]["port"])

    # Global default values
    if ("feedback" not in config["global"]): config["global"]["feedback"] = {"enable": False}
    
    if ("overwrite" not in config["global"]["feedback"]): config["global"]["feedback"]["overwrite"] = False
    else: config["global"]["feedback"]["overwrite"] = str2bool(config["global"]["feedback"]["overwrite"])    
    
    # Server default values
    if ("server" not in config): config["server"] = {}

    if ("loopforever" not in config["server"]): config["server"]["loopforever"] = False
    else: config["server"]["loopforever"] = str2bool(config["server"]["loopforever"])
    
    if ("logging" not in config["server"]): config["server"]["logging"] = True
    else: config["server"]["logging"] = str2bool(config["server"]["logging"])
    
    if ("verbose" not in config["server"]): config["server"]["verbose"] = False
    else: config["server"]["verbose"] = str2bool(config["server"]["verbose"])
            
    # Client default values
    if ("client" not in config): config["client"] = {}
    
    if ("logging" not in config["client"]): config["client"]["logging"] = True
    else: config["client"]["logging"] = str2bool(config["client"]["logging"])
    
    if ("verbose" not in config["client"]): config["client"]["verbose"] = False
    else: config["client"]["verbose"] = str2bool(config["client"]["verbose"])
    
    return config
    