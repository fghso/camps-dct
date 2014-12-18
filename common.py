# -*- coding: iso-8859-1 -*-

import socket
import json
import logging
import inspect
import calendar
import xmltodict
from datetime import datetime

    
# ==================== Classes ====================
class NetworkHandler():  
    def __init__(self, socketObject=None):
        if (socketObject): self.sock = socketObject
        else: self.sock = socket.socket()
        self.bufsize = 8192
        self.headersize = 10
    
    def _defaultSerializer(self, obj):
        #if isinstance(obj, datetime): return obj.isoformat()
        if isinstance(obj, datetime): return {"__datetime__": calendar.timegm(obj.utctimetuple())}
        elif isinstance(obj, set): return tuple(obj)
        raise TypeError("%s is not JSON serializable" % obj)
        
    def _defaultDeserializer(self, dictionary):
        if ("__datetime__" in dictionary): return datetime.utcfromtimestamp(dictionary["__datetime__"])
        return dictionary
        
    def connect(self, address, port):
        self.sock.connect((address, port))
        
    def getaddress(self):
        #return (socket.gethostbyaddr(self.sock.getpeername()[0])[0], self.sock.getpeername()[1])
        return (socket.gethostbyaddr(self.sock.getpeername()[0])[0].split(".")[0],) + self.sock.getpeername()
        
    def send(self, message):
        strMsg = json.dumps(message, default = self._defaultSerializer)
        msgSize = str(len(strMsg)).zfill(self.headersize)
        self.sock.sendall(msgSize + strMsg)
                
    def recv(self):
        # Get message size
        msgSize = ""
        while len(msgSize) < self.headersize:
            more = self.sock.recv(self.headersize)
            if (not more): return more
            msgSize += more
        msgSize = int(msgSize)
        
        # Get message
        strMsg = ""
        while len(strMsg) < msgSize:
            more = self.sock.recv(min(msgSize - len(strMsg), self.bufsize))
            if (not more): return more
            strMsg += more
        return json.loads(strMsg, object_hook = self._defaultDeserializer)
    
    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        
        
class EchoHandler():
    def __init__(self, configurationsDictionary = {}, loggingFileName = "", defaultLoggingLevel = "INFO"):
        self._extractConfig(configurationsDictionary)
        # Identify calling module
        frameRecords = inspect.stack()[1]
        self.callingModuleName = inspect.getmodulename(frameRecords[1])
        # Set up logging
        if (self.logging):
            if (not loggingFileName): loggingFileName = self.callingModuleName + ".log"
            logging.basicConfig(format=u"%(asctime)s %(name)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                                filename=loggingFileName, filemode="w", level=getattr(logging, defaultLoggingLevel))
            self.logger = logging.getLogger(self.callingModuleName)
        
    def _extractConfig(self, configurationsDictionary):
        if ("logging" not in configurationsDictionary): self.logging = True
        else: self.logging = str2bool(configurationsDictionary["logging"])
    
        if ("verbose" not in configurationsDictionary): self.verbose = False
        else: self.verbose = str2bool(configurationsDictionary["verbose"])
        
    def default(self, message, loggingLevel = ""):
        if (self.logging): self.logger.log(getattr(logging, loggingLevel, self.logger.getEffectiveLevel()), message)
        if (self.verbose): 
            if (loggingLevel): print loggingLevel + ": " + message + "\n",
            else: print message + "\n",
        
    def exception(self, message):
        if (self.logging): self.logger.exception(message)
        if (self.verbose): print "ERROR: " + message + "\n",
        
        
# ==================== Methods ====================
def str2bool(stringToConvert):
    if stringToConvert.lower() in ("true", "t", "yes", "y", "on", "1"): return True
    if stringToConvert.lower() in ("false", "f", "no", "n", "off", "0"): return False
    raise TypeError("The value '%s' is not considered a valid boolean in this context." % stringToConvert)
    
def loadConfig(configFilePath):
    configFile = open(configFilePath, "r")
    configDict = xmltodict.parse(configFile.read())
    config = configDict["config"]

    # Connection
    config["global"]["connection"]["port"] = int(config["global"]["connection"]["port"])

    # Global default values
    if ("feedback" not in config["global"]): config["global"]["feedback"] = False
    else: config["global"]["feedback"] = str2bool(config["global"]["feedback"])
    
    # Server default values
    if ("server" not in config): config["server"] = {}

    if ("loopforever" not in config["server"]): config["server"]["loopforever"] = False
    else: config["server"]["loopforever"] = str2bool(config["server"]["loopforever"])
    
    if ("filter" not in config["server"]): config["server"]["filter"] = []
    elif (not isinstance(config["server"]["filter"], list)): config["server"]["filter"] = [config["server"]["filter"]]
    
    for filter in config["server"]["filter"]:
        if ("parallel" not in filter): filter["parallel"] = False
        else: filter["parallel"] = str2bool(filter["parallel"]) 
            
    # Client default values
    if ("client" not in config): config["client"] = {}
    
    # Persistence
    if (isinstance(config["persistence"]["handler"], list)): 
        config["persistence"]["handler"] = config["persistence"]["handler"][0]
    
    return config
    