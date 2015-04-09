# -*- coding: iso-8859-1 -*-

"""Module to store shared code.

Functions and classes are put here when they are meant to be reused in different parts of the program. Some of them were built just for internal use, but others (listed bellow) can also be incorporated in user code as well (for example, in the :class:`crawler <crawler.DemoCrawler>` code).

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
    
    """
    
    defaultConfig = {"verbose": False, "logging": True, "loggingpath": "."}
    """Default configuration values. If no local value has been specified for an option in an instance, the value defined here for that option is used instead."""
    
    mandatoryConfig = {"verbose": None, "logging": None, "loggingpath": None}
    """Mandatory configuration values. If a value for an option here is ``None``, the handler respects the instance local configuration value for that option (or the default configuration value if no local value has been specified). Otherwise, the value specified here overrides any local value."""

    def __init__(self, configurationsDictionary = {}, loggingFileName = "", defaultLoggingLevel = "INFO"):
        """Constructor.
        
        Root logger configuration is done here using :func:`logging.basicConfig`. This means that *loggingFileName* and  *defaultLoggingLevel* parameters are defined by the first module in an import hierarchy that instantiate an :class:`EchoHandler` object, as subsequent calls to :func:`logging.basicConfig` have no effect. So, for example, when running the :doc:`client<client>`, it is the first module to instantiate an :class:`EchoHandler` object. The :doc:`client<client>` module imports the :mod:`crawler` module, but as :mod:`crawler` is the second module in the hierarchy, it will use the root logger configuration defined in :doc:`client<client>`, despite any local settings of *loggingFileName* and *defaultLoggingLevel*.
        
        Args:
            * *configurationsDictionary* (dict): Holds values for the three configuration options supported: verbose (bool), logging (bool) and loggingpath (str).
            * *loggingFileName* (str): Name of the file used to save logging messages.
            * *defaultLoggingLevel* (str): Level at which the root logger must be set. Supports any of the :ref:`level names <python:levels>` defined in Python's built-in logging module.
            
        .. seealso::
        
            Python's built-in :mod:`logging <python:logging>` module documentation.
        
        """
        self._extractConfig(configurationsDictionary)
        
        # Identify calling module
        frameRecords = inspect.stack()[1]
        self.callingModuleName = inspect.getmodulename(frameRecords[1])
        
        # Set up logging
        if not os.path.exists(self.loggingPath): os.makedirs(self.loggingPath)
        if (not loggingFileName): loggingFileName = self.callingModuleName + ".log"
        logging.basicConfig(format=u"%(asctime)s %(name)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                            filename=os.path.join(self.loggingPath, loggingFileName), filemode="w", level=getattr(logging, defaultLoggingLevel))
        self.logger = logging.getLogger(self.callingModuleName)
        
    def _extractConfig(self, configurationsDictionary):
        """Extract and store configurations.
        
        The configurations are extracted from *configurationsDictionary* and stored in separate instance variables. Each configuration has a default value, defined in :attr:`defaultConfig`, so it is possible to use :class:`EchoHandler` without specifying any of them. The values of :attr:`mandatoryConfig` are also checked here and, if set, override the values given in *configurationsDictionary*, as well as default values. 
        
        Args: 
            * *configurationsDictionary* (dict): Holds values for the three configuration options supported: verbose (bool), logging (bool) and loggingpath (str).
        
        """
        self.verbose = EchoHandler.defaultConfig["verbose"]
        self.logging = EchoHandler.defaultConfig["logging"]
        self.loggingPath = EchoHandler.defaultConfig["loggingpath"]
        
        if (configurationsDictionary):
            if ("verbose" in configurationsDictionary): 
                self.verbose = str2bool(configurationsDictionary["verbose"])
            if ("logging" in configurationsDictionary): 
                self.logging = str2bool(configurationsDictionary["logging"])
            if ("loggingpath" in configurationsDictionary): 
                self.loggingPath = configurationsDictionary["loggingpath"]
        
        if (EchoHandler.mandatoryConfig["verbose"] is not None): self.verbose = EchoHandler.mandatoryConfig["verbose"]
        if (EchoHandler.mandatoryConfig["logging"] is not None): self.logging = EchoHandler.mandatoryConfig["logging"]
        if (EchoHandler.mandatoryConfig["loggingpath"] is not None): self.loggingPath = EchoHandler.mandatoryConfig["loggingpath"]
        
    def out(self, message, loggingLevel = "", mode = "both"):
        """Log and/or print a message.
        
        Args: 
            * *message* (str): The message to be logged and/or printed.
            * *loggingLevel* (str): Level to use when logging the message. Supports any of the :ref:`level names <python:levels>` defined in Python's built-in logging module. If the level is bellow the default, no message is emited. If it is not specified, the default level is used.
            * *mode* (str): Control wether the message should be only logged, only printed or both. The corresponding accepted values are: "logonly", "printonly" and "both". This gives a fine-grained control, at the code level, over the output destination of the message.
        
        """
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
if (sys.platform == "win32"):
    import win32api, win32con
    def replace(src, dst):
        win32api.MoveFileEx(src, dst, win32con.MOVEFILE_REPLACE_EXISTING)
else: replace = os.rename

def str2bool(stringToConvert):
    """Convert a string to a boolean.
    
    Args:
        * *stringToConvert* (str): The following string values are accepted as ``True``: "true", "t", "yes", "y", "on", "1". The following string values are accepted as ``False``: "false", "f", "no", "n", "off", "0".
       
    Returns:
        ``True`` or ``False``, depending on input value.
        
    Raises:
        :exc:`TypeError<python:exceptions.TypeError>`: If input string is not one of the accepted values.
    
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
    
    # Echo
    config["global"]["echo"]["mandatory"] = EchoHandler.mandatoryConfig
    
    # Global default values
    if ("feedback" not in config["global"]): config["global"]["feedback"] = False
    else: config["global"]["feedback"] = str2bool(config["global"]["feedback"])
    
    if ("echo" in config["global"]):
        if ("verbose" in config["global"]["echo"]): 
            EchoHandler.defaultConfig["verbose"] = str2bool(config["global"]["echo"]["verbose"])
        
        if ("logging" in config["global"]["echo"]): 
            EchoHandler.defaultConfig["logging"] = str2bool(config["global"]["echo"]["logging"])
            
        if ("loggingpath" in config["global"]["echo"]): 
            EchoHandler.defaultConfig["loggingpath"] = config["global"]["echo"]["loggingpath"]
    
    # Server default values
    if ("echo" not in config["server"]): config["server"]["echo"] = {}

    if ("loopforever" not in config["server"]): config["server"]["loopforever"] = False
    else: config["server"]["loopforever"] = str2bool(config["server"]["loopforever"])
    
        # Filters
    if ("filtering" not in config["server"]): config["server"]["filtering"] = {"filter": []}
    if (not isinstance(config["server"]["filtering"]["filter"], list)): config["server"]["filtering"]["filter"] = [config["server"]["filtering"]["filter"]]
    
    for filter in config["server"]["filtering"]["filter"]:
        if ("parallel" not in filter): filter["parallel"] = False
        else: filter["parallel"] = str2bool(filter["parallel"])
        
    # Client default values
    if ("echo" not in config["client"]): config["client"]["echo"] = {}
            
    return config
    