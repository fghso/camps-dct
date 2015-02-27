# -*- coding: iso-8859-1 -*-

"""Module to store shared code.

Functions and classes are put here when they are meant to be reused in different parts of the program. Some of them were built just for internal use, but others (listed bellow) can also be in user code as well (for example, in crawler code).

Class available to use:
    EchoHandler(): Handler to manage logging and printing of messages.
    
Function available to use: 
    str2bool(stringToConvert): Auxiliary function to convert strings to booleans.

"""

import sys
import os
import socket
import traceback
import inspect
import json
import logging
import calendar
import xmltodict
from datetime import datetime

    
# ==================== Classes ====================
class EchoHandler():
    """Manage logging and printing of messages.
    
    Provides a unified and common infrastructure for information output in different parts of the program, besides abstracting low level details about output operations. 
    
    Attributes:
        globalConfig (dict): Mandatory configuration values. If a value for an option here is None, the handler respects the 
            instance local configuration value for that option. Otherwise, the value specified here overrides any local value.
    
    """
    globalConfig = {"verbose": None, "logging": None, "loggingpath": None}

    def __init__(self, configurationsDictionary = {}, loggingFileName = "", defaultLoggingLevel = "INFO"):
        """Constructor.
        
        The root logger configuration is done here using logging.basicConfig method. This means that loggingFileName and  defaultLoggingLevel options are defined by the first module in an import hierarchy that instantiate an EchoHandler object, as subsequent calls to logging.basicConfig have no effect (See Python's logging module documentation for details). So, for example, when running client.py it is the first module to instantiate an EchoHandler object. client.py imports crawler.py module, but as crawler.py is the second module in the hierarchy, it will use the root logger configuration defined in client.py, despite any local settings of loggingFileName and defaultLoggingLevel used.
        
        Args:
            configurationsDictionary (dict): Holds values for the 3 configuration options supported: verbose (bool), 
                logging (bool) and loggingpath (str).
            loggingFileName (str): Name of the file used to save logging messages.
            defaultLoggingLevel (str): Level to set for the root logger. See Python's logging module documentation 
                for details.
        
        """
        self._extractConfig(configurationsDictionary)
        
        # Identify calling module
        frameRecords = inspect.stack()[1]
        self.callingModuleName = inspect.getmodulename(frameRecords[1])
        
        # Set up logging
        if (EchoHandler.globalConfig["loggingpath"]): self.loggingPath = EchoHandler.globalConfig["loggingpath"]
        if not os.path.exists(self.loggingPath): os.makedirs(self.loggingPath)
        if (not loggingFileName): loggingFileName = self.callingModuleName + ".log"
        logging.basicConfig(format=u"%(asctime)s %(name)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                            filename=os.path.join(self.loggingPath, loggingFileName), filemode="w", level=getattr(logging, defaultLoggingLevel))
        self.logger = logging.getLogger(self.callingModuleName)
        
    def _extractConfig(self, configurationsDictionary):
        """Extract and store configurations.
        
        
        
        """
        self.verbose = False
        self.logging = True
        self.loggingPath = "."
        
        if (configurationsDictionary):
            if ("verbose" in configurationsDictionary): 
                self.verbose = str2bool(configurationsDictionary["verbose"])
            if ("logging" in configurationsDictionary): 
                self.logging = str2bool(configurationsDictionary["logging"])
            if ("loggingpath" in configurationsDictionary): 
                self.loggingPath = configurationsDictionary["loggingpath"]
        
        if (EchoHandler.globalConfig["verbose"] is not None): self.verbose = EchoHandler.globalConfig["verbose"]
        if (EchoHandler.globalConfig["logging"] is not None): self.logging = EchoHandler.globalConfig["logging"]
        if (EchoHandler.globalConfig["loggingpath"] is not None): self.loggingPath = EchoHandler.globalConfig["loggingpath"]
        
    def out(self, message, loggingLevel = "", mode = "both"):
        if (self.logging) and (mode != "printonly"): 
            if (loggingLevel == "EXCEPTION"): self.logger.exception(message)
            else: self.logger.log(getattr(logging, loggingLevel, self.logger.getEffectiveLevel()), message)
            
        if (self.verbose) and (mode != "logonly"):
            if (loggingLevel == "EXCEPTION"):
                print "EXCEPTION: %s\n" % message,
                traceback.print_exc()
            elif (loggingLevel): print "%s: %s\n" % (loggingLevel, message),
            else: print "%s\n" % message,
        
        
class NetworkHandler():  
    def __init__(self, socketObject=None):
        if (socketObject): self.sock = socketObject
        else: self.sock = socket.socket()
        self.bufsize = 8192
        self.headersize = 10
    
    def _defaultSerializer(self, obj):
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
        
        
# ==================== Methods ====================
if sys.platform == "win32":
    import win32api, win32con
    def replace(src, dst):
        win32api.MoveFileEx(src, dst, win32con.MOVEFILE_REPLACE_EXISTING)
else: replace = os.rename

def str2bool(stringToConvert):
    """Convert strings to booleans.
    
    Args:
        stringToConvert (str): 
            The following string values are acepted as True: "true", "t", "yes", "y", "on", "1". 
            The following string values are acepted as False: "false", "f", "no", "n", "off", "0".
       
    Returns:
        True or False, depending on input value.
        
    Raises:
        TypeError: If input string is not one of the acepted values.
    
    """
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
    
    if ("echo" in config["global"]):
        if ("verbose" in config["global"]["echo"]): 
            EchoHandler.globalConfig["verbose"] = str2bool(config["global"]["echo"]["verbose"])
        
        if ("logging" in config["global"]["echo"]): 
            EchoHandler.globalConfig["logging"] = str2bool(config["global"]["echo"]["logging"])
            
        if ("loggingpath" in config["global"]["echo"]): 
            EchoHandler.globalConfig["loggingpath"] = config["global"]["echo"]["loggingpath"]
            
    config["global"]["echo"] = EchoHandler.globalConfig
    
    # Server default values
    if ("echo" not in config["server"]): config["server"]["echo"] = {}

    if ("loopforever" not in config["server"]): config["server"]["loopforever"] = False
    else: config["server"]["loopforever"] = str2bool(config["server"]["loopforever"])
    
        # Persistence
    if (isinstance(config["server"]["persistence"]["handler"], list)): 
        config["server"]["persistence"]["handler"] = config["server"]["persistence"]["handler"][0]
        
        # Filters
    if ("filtering" not in config["server"]): config["server"]["filtering"] = {"filter": []}
    if (not isinstance(config["server"]["filtering"]["filter"], list)): config["server"]["filtering"]["filter"] = [config["server"]["filtering"]["filter"]]
    
    for filter in config["server"]["filtering"]["filter"]:
        if ("parallel" not in filter): filter["parallel"] = False
        else: filter["parallel"] = str2bool(filter["parallel"])
        
    # Client default values
    if ("echo" not in config["client"]): config["client"]["echo"] = {}
            
    return config
    