# -*- coding: iso-8859-1 -*-

"""Module to store persistence handler classes.

Persistence handlers take care of all implementation details related to resource storage. They expose a common interface (defined in :class:`BasePersistenceHandler`) through which the server (and/or filters/crawlers) can load, save and perform other operations over resources independently from where and how the resources are actually stored. At any point in time, the collection status of each resource must be one of those defined in the struct-like class :class:`StatusCodes`.

"""

import os
import threading
import tempfile
import cStringIO
import glob
import re
import json
import csv
import common
import mysql.connector
import Queue
from datetime import datetime
from copy import deepcopy
from collections import deque


class StatusCodes():
    """A struct-like class to hold constants for resources status codes.
    
    The numeric value of each code can be modified to match the one used in the final location where the resources are persisted. The name of each code (``SUCCEEDED``, ``INPROGRESS``, ``AVAILABLE``, ``FAILED``, ``ERROR``) must not be modified.
    
    """
    SUCCEEDED  =  2
    INPROGRESS =  1
    AVAILABLE  =  0 
    FAILED     = -1
    ERROR      = -2


class BasePersistenceHandler(): 
    """Abstract class. All persistence handlers should inherit from it or from other class that inherits."""
 
    def __init__(self, configurationsDictionary): 
        """Constructor.  
        
        Each persistence handler receives everything in its corresponding handler section of the XML configuration file as the parameter *configurationsDictionary*. 
        
        """
        self._extractConfig(configurationsDictionary)
        self.status = StatusCodes() 
        
    def _extractConfig(self, configurationsDictionary):
        """Extract and store configurations.
        
        If some configuration needs any kind of pre-processing, it is done here. Extend this method if you need to pre-process custom configuration options.
        
        """
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
        
    def setup(self):
        """Execute per client initialization procedures.
        
        This method is called every time a connection to a new client is opened, allowing to execute initialization code on a per client basis (which differs from :meth:`__init__` that is called when the server instantiate the persistence handler, i.e., :meth:`__init__` is called just one time for the whole period of execution of the program).
        
        """
        pass
    
    def select(self): 
        """Retrive an ``AVAILABLE`` resource.
        
        Returns: 
            A tuple in the format (*resourceKey*, *resourceID*, *resourceInfo*).
        
            * *resourceKey* (user defined type): Value that uniquely identify the resource internally. It works like a primary key in relational databases and makes possible the existence of resources with the same ID, if needed.
            * *resourceID* (user defined type): Resource ID to be sent to a client.
            * *resourceInfo* (dict): Other information related to the resource, if there is any.
        
        """
        return (None, None, None)
    
    def update(self, resourceKey, status, resourceInfo): 
        """Update the specified resource, setting its status and information data to the ones given.
        
        Args: 
            * *resourceKey* (user defined type): Value that uniquely identify the resource internally.
            * *status* (:class:`StatusCodes`): New status of the resource.
            * *resourceInfo* (dict): Other information related to the resource, if there is any.
            
        """
        pass
    
    def insert(self, resourcesList): 
        """Insert new resources into the final location where resources are persisted.
        
        Args: 
            * *resourcesList* (list): List of tuples containing all new resources to be inserted. Each resource is defined by a tuple in the format (*resourceID*, *resourceInfo*).
            
        """
        pass
    
    def count(self): 
        """Count the number of resources in each status category.
        
        Returns: 
            A tuple in the format (*total*, *succeeded*, *inprogress*, *available*, *failed*, *error*) where all fields are integers representing the number of resources with the respective status code.
            
        """
        return (0, 0, 0, 0, 0, 0)
    
    def reset(self, status): 
        """Change to ``AVAILABLE`` all resources with the status code given.
        
        Args:
            * *status* (:class:`StatusCodes`): Status of the resources to be reseted.
        
        Returns:
            Number of resources reseted.
        
        """
        return 0
    
    def finish(self): 
        """Execute per client finalization procedures.
        
        This method is called every time a connection to a client is closed, allowing to execute finalization code on a per client basis. It is the counterpart of :meth:`setup`.
        
        """
        pass
    
    def shutdown(self): 
        """Execute program finalization procedures (similar to a destructor).
        
        This method is called when the server is shut down, allowing to execute finalization code in a global manner. It is intended to be the counterpart of :meth:`__init__`, but differs from :meth:`__del__() <python:object.__del__>` in that it is not bounded to the live of the persistence handler object itself, but rather to the span of execution time of the server.
        
        """
        pass
        
        
# IMPORTANT NOTE: MemoryPersistenceHandler class was built as basis for FilePersistenceHandler and its extensions, 
# and for test purposes. Altough it can be set in the configuration file, it is not intended for direct use in a 
# production enviroment. In this case, choose one of the file based handlers instead
class MemoryPersistenceHandler(BasePersistenceHandler):
    def __init__(self, configurationsDictionary): 
        BasePersistenceHandler.__init__(self, configurationsDictionary)
        self.insertLock = threading.Lock()
        self.resources = []
        self.IDsHash = {}
        self.statusRecords = {self.status.SUCCEEDED:  [],
                              self.status.INPROGRESS: [],
                              self.status.AVAILABLE:  deque(), 
                              self.status.FAILED:     [],
                              self.status.ERROR:      []}
        #self._loadTestData()
            
    def _extractConfig(self, configurationsDictionary):
        BasePersistenceHandler._extractConfig(self, configurationsDictionary)
    
        if ("uniqueresourceid" not in self.config): self.config["uniqueresourceid"] = False
        else: self.config["uniqueresourceid"] = common.str2bool(self.config["uniqueresourceid"])
    
        if ("onduplicateupdate" not in self.config): self.config["onduplicateupdate"] = False
        else: self.config["onduplicateupdate"] = common.str2bool(self.config["onduplicateupdate"])
        
    def _save(self, pk, id, status, info, changeInfo = True):
        if (pk is not None):
            if (status is not None): self.resources[pk]["status"] = status
            if (changeInfo): 
                if (self.resources[pk]["info"] is not None) and (info is not None): self.resources[pk]["info"].update(info)
                else: self.resources[pk]["info"] = info
        else: 
            self.resources.append({"id": id, "status": status, "info": info})
            
    def _loadTestData(self):
        self.resources.extend([
            {"id": 1, "status": 0, "info": {"crawler_name": "c1", "response_code": 3}},
            {"id": 2, "status": 0, "info": {"crawler_name": "c2", "response_code": 3}},
            {"id": 3, "status": 0, "info": None},
            {"id": 4, "status": 0, "info": None}
        ])
        for pk, resource in enumerate(self.resources):
            self.statusRecords[resource["status"]].append(pk)
            if (self.config["uniqueresourceid"]): 
                if (resource["id"] not in self.IDsHash): self.IDsHash[resource["id"]] = pk
                else: raise KeyError("Duplicated ID found in resources list: %s." % resource["id"])
    
    def select(self): 
        try: pk = self.statusRecords[self.status.AVAILABLE].popleft()
        except IndexError: return (None, None, None)
        self._save(pk, None, self.status.INPROGRESS, None, False)
        self.statusRecords[self.status.INPROGRESS].append(pk)
        return (pk, self.resources[pk]["id"], deepcopy(self.resources[pk]["info"]))
    
    def update(self, resourceKey, status, resourceInfo): 
        currentStatus = self.resources[resourceKey]["status"]
        self.statusRecords[currentStatus].remove(resourceKey)
        if (resourceInfo): self._save(resourceKey, None, status, resourceInfo)
        else: self._save(resourceKey, None, status, resourceInfo, False)
        self.statusRecords[status].append(resourceKey)
        
    def insert(self, resourcesList): 
        for resourceID, resourceInfo in resourcesList:
            if (self.config["uniqueresourceid"]) and (resourceID in self.IDsHash):
                if (self.config["onduplicateupdate"]): 
                    self._save(self.IDsHash[resourceID], None, None, resourceInfo)
                    continue
                else: raise KeyError("Cannot insert resource, ID %s already exists." % resourceID)
            with self.insertLock:
                self.statusRecords[self.status.AVAILABLE].append(len(self.resources))
                if (self.config["uniqueresourceid"]): self.IDsHash[resourceID] = len(self.resources)
                self._save(None, resourceID, self.status.AVAILABLE, resourceInfo)
        
    def count(self): 
        return (len(self.resources), 
                len(self.statusRecords[self.status.SUCCEEDED]), 
                len(self.statusRecords[self.status.INPROGRESS]), 
                len(self.statusRecords[self.status.AVAILABLE]), 
                len(self.statusRecords[self.status.FAILED]), 
                len(self.statusRecords[self.status.ERROR]))
        
    def reset(self, status): 
        resetList = self.statusRecords[status][:]
        for pk in resetList:
            self.statusRecords[status].remove(pk)
            self._save(pk, None, self.status.AVAILABLE, None, False)
            self.statusRecords[self.status.AVAILABLE].appendleft(pk)
        return len(resetList)
            
        
class FilePersistenceHandler(MemoryPersistenceHandler):
    """Load and dump resources from/to a file.
    
    All resources in the file are loaded into memory before the server operations begin. So, this handler is recomended for small to medium size datasets that can be completely fitted into machine's memory. For larger datasets, consider using another persistence handler. Another option for large datasets is to divide the resources in more than one file, collecting the resources of one file at a time.
    
    The default version of this handler supports CSV and JSON files. It is possible to add support to other file types by subclassing :class:`BaseFileColumns` and :class:`BaseFileHandler`. The new file type must also be included in  the :attr:`supportedFileTypes` dictionary.
    
    """
    class BaseFileColumns():
        """Hold column names of data in the file, allowing fast access to names of ID, status and info columns."""
        
        def __init__(self, fileName, idColumn, statusColumn):
            self.names = self._extractColNames(fileName)
            self.idName = idColumn
            self.statusName = statusColumn
            self.infoNames = [name for name in self.names if (name not in (self.idName, self.statusName))]
            
        def _extractColNames(self, fileName): 
            """Extract column names from the file.
            
            Must be overriden, as column names extraction depends on the file type.
            
            Returns: 
                A list of all column names in the file.
        
            """
            return [] 

    class BaseFileHandler():
        """Handle low level details about persistence in a specific file type.
    
        Each resource loaded from a file is stored in memory in a dictionary in the format ``{"id": X, "status": X, "info": {...}}``, which is the resource internal representation format. This handler is responsible for translating resources in the internal representation format to the format used in a specific file type and vice-versa.
        
        """
        def __init__(self): self.status = StatusCodes() 
        
        def parse(self, resource, columns): 
            """Transform resource from file format to internal representation format.
            
            Args: 
                * *resource* (file specific type): Resource given in file format.
                * *columns* (:class:`BaseFileColumns <FilePersistenceHandler.BaseFileColumns>` subclass): Object holding column names.
            
            Returns:
                A resource in internal representation format.
            
            """
            return {"id": None, "status": None, "info": None}
            
        def unparse(self, resource, columns): 
            """Transform resource from internal representation format to file format.
            
            Args: 
                * *resource* (dict): Resource given in internal representation format.
                * *columns* (:class:`BaseFileColumns <FilePersistenceHandler.BaseFileColumns>` subclass): Object holding column names.
            
            Returns:
                A resource in file format.
            
            """
            return None
            
        def load(self, file, columns): 
            """Load resources in file format and yield them in internal representation format.
            
            Args: 
                * *file* (:ref:`file object<python:bltin-file-objects>`): File object bounded to the physical file where resources are stored.
                * *columns* (:class:`BaseFileColumns <FilePersistenceHandler.BaseFileColumns>` subclass): Object holding column names.
                
            Yields:
                A resource in internal representation format.
            
            """
            yield {"id": None, "status": None, "info": None} 
            
        def dump(self, resources, file, columns):
            """Save resources in internal representation format to file format.
            
            Args: 
                * *resources* (list): List of resources in internal representation format.
                * *file* (:ref:`file object<python:bltin-file-objects>`): File object bounded to the physical file where resources will be stored.
                * *columns* (:class:`BaseFileColumns <FilePersistenceHandler.BaseFileColumns>` subclass): Object holding column names.

            """        
            pass
            
    class CSVColumns(BaseFileColumns):
        """Hold column names of data in CSV files, allowing fast access to names of ID, status and info columns."""
    
        def _extractColNames(self, fileName):
            with open(fileName, "r") as file: 
                reader = csv.DictReader(file, quoting = csv.QUOTE_MINIMAL, quotechar = "'", skipinitialspace = True)
                columns = reader.fieldnames
            return [col.strip("\"") for col in columns]
    
    class CSVHandler(BaseFileHandler):
        """Handle low level details about persistence in CSV files.
        
        .. note::
            
            This class and :class:`CSVColumns <FilePersistenceHandler.CSVColumns>` class uses Python's built-in :mod:`python:csv` module internally. 
        
        """
        def _parseValue(self, value):
            if (not value): return None
            if (not value.startswith("\"")):
                if value.upper() in ("TRUE", "T"): return True
                if value.upper() in ("FALSE", "F"): return False       
                if value.upper() in ("NONE", "NULL"): return None
                if ("." in value): return float(value)
                return int(value)
            return value.strip("\"") 
        
        def _unparseValue(self, value):
            if isinstance(value, basestring): 
                if isinstance(value, unicode): value = value.encode("utf-8")
                return "".join(("\"", value, "\""))
            if isinstance(value, bool): return ("T" if (value) else "F")
            return value
            
        def parse(self, resource, columns):
            parsed = {"id": self._parseValue(resource[columns.idName])}
            if ((columns.statusName in columns.names) and (resource[columns.statusName])): 
                parsed["status"] = self._parseValue(resource[columns.statusName])
            else: parsed["status"] = self.status.AVAILABLE
            if (columns.infoNames):
                parsed["info"] = {}
                for column in columns.infoNames:
                    parsed["info"][column] = self._parseValue(resource[column])
            return parsed
        
        def unparse(self, resource, columns):
            buffer = cStringIO.StringIO()
            writer = csv.DictWriter(buffer, columns.names, quoting = csv.QUOTE_MINIMAL, quotechar = "'", lineterminator = "\n", extrasaction = "ignore")
            unparsed = {columns.idName: self._unparseValue(resource["id"])}
            if (resource["status"] != self.status.AVAILABLE): 
                unparsed[columns.statusName] = self._unparseValue(resource["status"])
            if (resource["info"]):
                for key, value in resource["info"].iteritems():
                    if (value is not None) and (key in columns.infoNames): unparsed[key] = self._unparseValue(value)
            writer.writerow(unparsed)
            return buffer.getvalue()
            
        def load(self, file, columns):
            reader = csv.DictReader(file, columns.names, quoting = csv.QUOTE_MINIMAL, quotechar = "'", skipinitialspace = True)
            next(reader)
            for resource in reader:
                yield self.parse(resource, columns)
        
        def dump(self, resources, file, columns):
            writer = csv.DictWriter(file, columns.names, quoting = csv.QUOTE_MINIMAL, quotechar = "'", lineterminator = "\n", extrasaction = "ignore")
            writer.writeheader()
            # In case of CSV, it is easier and faster to unparse the resource here instead of using 
            # unparse method, so we can use writerow method to directly save the resource to file
            for resource in resources:
                row = {columns.idName: self._unparseValue(resource["id"])}
                if (resource["status"] != 0): row[columns.statusName] = self._unparseValue(resource["status"])
                if (resource["info"]):
                    for key, value in resource["info"].iteritems():
                        if (value is not None) and (key in columns.infoNames): row[key] = self._unparseValue(value)
                writer.writerow(row)
        
    class JSONColumns(BaseFileColumns):
        """Hold column names of data in JSON files, allowing fast access to names of ID, status and info columns."""
    
        def _extractColNames(self, fileName):
            with open(fileName, "r") as file: content = file.read(1024)
            columnsStart = content.index("[") + 1
            columnsEnd = content.index("]")
            columns = content[columnsStart:columnsEnd]
            return [name.strip("\" ") for name in columns.split(",")]
       
    class JSONHandler(BaseFileHandler):    
        """Handle low level details about persistence in JSON files.
        
        .. note::
            
            This class and :class:`JSONColumns <FilePersistenceHandler.JSONColumns>` uses Python's built-in :mod:`python:json` module internally. 
        
        """
        def parse(self, resource, columns):
            parsed = {"id": resource[columns.idName]}
            if ((columns.statusName in columns.names) and (columns.statusName in resource)): 
                parsed["status"] = resource[columns.statusName]
            else: parsed["status"] = self.status.AVAILABLE
            if (columns.infoNames):
                parsed["info"] = {}
                for column in columns.infoNames:
                    if (column in resource): parsed["info"][column] = resource[column]
                    else: parsed["info"][column] = None
            return parsed
        
        def unparse(self, resource, columns):
            unparsed = {columns.idName: resource["id"]}
            if (resource["status"] != self.status.AVAILABLE): unparsed[columns.statusName] = resource["status"]
            if (resource["info"]): 
                for key, value in resource["info"].iteritems(): 
                    if (value is not None) and (key in columns.infoNames): unparsed[key] = value
            return json.dumps(unparsed)
    
        def load(self, file, columns): 
            input = json.load(file)
            for resource in input["resources"]: 
                yield self.parse(resource, columns)

        def dump(self, resources, file, columns):
            file.write("{\"columns\": %s, \"resources\": [" % json.dumps(columns.names))
            separator = ""
            for resource in resources:
                file.write("%s%s" % (separator, self.unparse(resource, columns)))
                separator = ", "
            file.write("]}")
            
    supportedFileTypes = {
                         # Type   : [FileColumns, FileHandler]
                           "CSV"  : ["CSVColumns", "CSVHandler"],
                           "JSON" : ["JSONColumns", "JSONHandler"]
                         }
    """Associate file types and its columns and handler classes. The type of the current file is provided by the user directly (through the ``filetype`` option in the XML configuration file) or indirectly (through the file extension extracted from file name). When checking if the type of the current file is on the list of supported file types, the comparison between the strings is case insensitive."""
                         
    def __init__(self, configurationsDictionary): 
        MemoryPersistenceHandler.__init__(self, configurationsDictionary)
        self.echo = common.EchoHandler(self.config["echo"])
        self.saveLock = threading.Lock()
        self.dumpExceptionEvent = threading.Event()
        self.timer = threading.Timer(self.config["savetimedelta"], self.timelyDump)
        self.timer.daemon = True
        self._setFileHandler()
        file = open(self.config["filename"], "r")
        try:
            resourcesList = self.fileHandler.load(file, self.fileColumns)
            for resource in resourcesList:
                self.statusRecords[resource["status"]].append(len(self.resources))
                if (self.config["uniqueresourceid"]): 
                    if (resource["id"] not in self.IDsHash): self.IDsHash[resource["id"]] = len(self.resources)
                    else: raise KeyError("Duplicated ID found in '%s': %s." % (self.config["filename"], resource["id"])) 
                if ("info" not in resource): resource["info"] = None
                self.resources.append(resource)
        except: raise
        finally: file.close()
        self.timer.start()
        
    def _extractConfig(self, configurationsDictionary):
        MemoryPersistenceHandler._extractConfig(self, configurationsDictionary)
        
        if ("filetype" in self.config): self.config["filetype"] = self.config["filetype"].lower()
        else: self.config["filetype"] = os.path.splitext(self.config["filename"])[1][1:].lower()
        
        self.config["savetimedelta"] = int(self.config["savetimedelta"])
        if (self.config["savetimedelta"] < 1): raise ValueError("Parameter 'savetimedelta' must be greater than zero.")
    
    def _save(self, pk, id, status, info, changeInfo = True):
        with self.saveLock: MemoryPersistenceHandler._save(self, pk, id, status, info, changeInfo)
    
    def _setFileHandler(self):
        for type, handler in FilePersistenceHandler.supportedFileTypes.iteritems():
            if (self.config["filetype"] == type.lower()): 
                FileColumnsClass = getattr(self, handler[0])
                FileHandlerClass = getattr(self, handler[1])
                self.fileColumns = FileColumnsClass(self.config["filename"], self.config["resourceidcolumn"], self.config["statuscolumn"])
                self.fileHandler = FileHandlerClass()
                return            
        raise TypeError("Unknown file type '%s' for file '%s'." % (self.config["filetype"], self.config["filename"]))
            
    def _checkDumpException(function):
        def decoratedFunction(self, *args):
            if (self.dumpExceptionEvent.is_set()): 
                raise RuntimeError("Exception on dump thread. Execution of FilePersistenceHandler aborted.")
            return function(self, *args)
        return decoratedFunction
                
    def dump(self):
        self.echo.out("Saving list of resources to file '%s'..." % self.config["filename"])
        with tempfile.NamedTemporaryFile(mode = "w", suffix = ".temp", prefix = "dump_", dir = "", delete = False) as temp: 
            with self.saveLock:
                self.fileHandler.dump(self.resources, temp, self.fileColumns)
        common.replace(temp.name, self.config["filename"])
        self.echo.out("Done.")
        
    def timelyDump(self):
        try: 
            self.dump()
        except:
            self.dumpExceptionEvent.set()
            raise
        self.timer = threading.Timer(self.config["savetimedelta"], self.timelyDump)
        self.timer.daemon = True
        self.timer.start()
        
    @_checkDumpException
    def select(self): 
        return MemoryPersistenceHandler.select(self)

    @_checkDumpException
    def update(self, resourceKey, status, resourceInfo): 
        return MemoryPersistenceHandler.update(self, resourceKey, status, resourceInfo)
    
    @_checkDumpException
    def insert(self, resourcesList): 
        return MemoryPersistenceHandler.insert(self, resourcesList)
    
    @_checkDumpException
    def count(self): 
        return MemoryPersistenceHandler.count(self)
    
    @_checkDumpException
    def reset(self, status): 
        return MemoryPersistenceHandler.reset(self, status)
                
    def shutdown(self): 
        self.timer.cancel()
        self.dump()
        
        
class RolloverFilePersistenceHandler(FilePersistenceHandler):
    """Load and dump resources from/to files respecting limits of file size and/or number of resources per file.
    
    This handler uses multiple instances of :class:`FilePersistenceHandler` to allow insertion of new resources respecting limits specified by the user. It is also capable of reading and updating resources from multiple files.
    
    The rollover handler leaves the low level details of persistence for the file handlers attached to each file, taking care of the coordination necessary to maintain consistency between them and also of the verification of limits established. 
    
    When inserting new resources, every time the file size limit and/or number of resources per file limit is reached rollover handler opens a new file and assigns a new instance of :class:`FilePersistenceHandler` to handle it. All resources, however, are maintained in memory. So, as in the case of :class:`FilePersistenceHandler`, this handler is not well suited for large datasets that cannot be completely fitted in memory.
    
    .. note::
    
        This handler was inspired by Python's :class:`python:logging.handlers.RotatingFileHandler` class.
    
    """
    def __init__(self, configurationsDictionary): 
        self.originalConfig = deepcopy(configurationsDictionary)
        MemoryPersistenceHandler.__init__(self, configurationsDictionary)
        self._setFileHandler()
        self.fileHandlersList = []
        self.nextSuffixNumber = 1
        self.insertHandlerIndex = 0
        self.insertSize = -1
        self.insertAmount = -1
        
        # Iterate over old rollover files to get file names and max suffix number already used
        fileNamesList = [self.config["filename"]]
        for name in glob.iglob(self.config["filename"] + ".*"):
            if re.search("\.[0-9]+$", name):
                fileNamesList.append(name)
                suffixNumber = int(name.rsplit(".", 1)[1])
                if (suffixNumber >= self.nextSuffixNumber): self.nextSuffixNumber = suffixNumber + 1
        
        # Initialize file persistence handlers
        for fileName in fileNamesList: self._addHandler(fileName)
        
        # Get initial file size and amount
        if (self.config["sizethreshold"]): self.insertSize = os.path.getsize(self.config["filename"])
        if (self.config["amountthreshold"]): self.insertAmount = len(self.fileHandlersList[self.insertHandlerIndex].resources)
                    
    def _extractConfig(self, configurationsDictionary):
        FilePersistenceHandler._extractConfig(self, configurationsDictionary)
    
        if ("sizethreshold" not in self.config): self.config["sizethreshold"] = 0
        else: self.config["sizethreshold"] = int(self.config["sizethreshold"])
        
        if ("amountthreshold" not in self.config): self.config["amountthreshold"] = 0
        else: self.config["amountthreshold"] = int(self.config["amountthreshold"])
        
        if (self.config["sizethreshold"] < 0): raise ValueError("Parameter 'sizethreshold' must be zero or greater.")
        if (self.config["amountthreshold"] < 0): raise ValueError("Parameter 'amountthreshold' must be zero or greater.")
        if (self.config["sizethreshold"] == 0) and (self.config["amountthreshold"] == 0): 
            raise ValueError("Parameters 'sizethreshold' and 'amountthreshold' cannot be zero at the same time.")
            
    def _addHandler(self, fileName):
        config = deepcopy(self.originalConfig)
        config["filename"] = fileName
        config["filetype"] = self.config["filetype"]
        handler = FilePersistenceHandler(config)
        if (self.config["uniqueresourceid"]): 
            duplicated = set(handler.IDsHash).intersection(self.IDsHash)
            if (not duplicated): self.IDsHash.update(dict.fromkeys(handler.IDsHash, len(self.fileHandlersList)))
            else:
                details = ["%s ['%s']" % (resourceID, self.fileHandlersList[self.IDsHash[resourceID]].config["filename"]) for resourceID in duplicated]
                raise KeyError("Duplicated ID(s) found in '%s': %s" % (fileName, ", ".join(details))) 
        self.fileHandlersList.append(handler)

    def select(self): 
        for handlerKey, handler in enumerate(self.fileHandlersList): 
            (resourceKey, resourceID, resourceInfo) = handler.select()
            if (resourceID): return ((handlerKey, resourceKey), resourceID, resourceInfo)
        return (None, None, None)    
    
    def update(self, keyPair, status, resourceInfo): 
        self.fileHandlersList[keyPair[0]].update(keyPair[1], status, resourceInfo)
    
    def insert(self, resourcesList): 
        for resourceID, resourceInfo in resourcesList:
            if (self.config["uniqueresourceid"]) and (resourceID in self.IDsHash):
                handler = self.fileHandlersList[self.IDsHash[resourceID]]
                try: handler.insert([(resourceID, resourceInfo)])
                except KeyError: raise KeyError("Cannot insert resource, ID %s already exists in '%s'." % (resourceID, handler.config["filename"]))
                continue
        
            with self.insertLock:
                handler = self.fileHandlersList[self.insertHandlerIndex]
      
                # Change insert handler if size or amount thresholds were exceeded. If there is no more
                # handlers in the list, open a new file and instantiate a new handler to take care of it
                while ((self.insertSize >= self.config["sizethreshold"]) or 
                       (self.insertAmount >= self.config["amountthreshold"])):
                    self.insertHandlerIndex += 1
                    if (self.insertHandlerIndex >= len(self.fileHandlersList)): 
                        newFileName = "%s.%d" % (self.config["filename"], self.nextSuffixNumber)
                        with open(newFileName, "w") as file: self.fileHandler.dump([], file, self.fileColumns)
                        self._addHandler(newFileName)
                        self.nextSuffixNumber += 1
                    handler = self.fileHandlersList[self.insertHandlerIndex]    
                    if (self.config["sizethreshold"]): self.insertSize = os.path.getsize(handler.config["filename"])
                    if (self.config["amountthreshold"]): self.insertAmount = len(handler.resources)
                
                handler.insert([(resourceID, resourceInfo)])
                if (self.config["uniqueresourceid"]): self.IDsHash[resourceID] = self.insertHandlerIndex
                
                if (self.config["sizethreshold"]): 
                    self.insertSize += len(self.fileHandler.unparse(handler.resources[-1], self.fileColumns))
                if (self.config["amountthreshold"]): 
                    self.insertAmount += 1
    
    def count(self):
        counts = [0] * 6
        for handler in self.fileHandlersList: 
            counts = [x + y for x, y in zip(counts, handler.count())]
        return counts
    
    def reset(self, status): 
        for handler in self.fileHandlersList: handler.reset(status)
        
    def shutdown(self): 
        for handler in self.fileHandlersList: handler.shutdown()
        
        
class MySQLPersistenceHandler(BasePersistenceHandler):
    """Store and retrieve resources to/from a MySQL database. 
    
    The table must already exist in the database and must contain at least three columns: a primary key column, a resource ID column and a status column.
    
    .. note::
    
        This handler uses `MySQL Connector/Python <http://dev.mysql.com/doc/connector-python/en/index.html>`_ to interact with MySQL databases. 
    
    """
    def __init__(self, configurationsDictionary):
        BasePersistenceHandler.__init__(self, configurationsDictionary)
        self.echo = common.EchoHandler(self.config["echo"])
        self.local = threading.local()
        self.selectCacheThreadExceptionEvent = threading.Event()
        self.selectEmptyEvent = threading.Event()
        
        connection = mysql.connector.connect(**self.config["connargs"])
        cursor = connection.cursor()
        
        # Get column names
        query = "SELECT * FROM " + self.config["table"] + " LIMIT 0"
        cursor.execute(query)
        cursor.fetchall()
        self.colNames = cursor.column_names
        self.excludedColNames = (self.config["primarykeycolumn"], self.config["resourceidcolumn"], self.config["statuscolumn"])
        self.infoColNames = [name for name in self.colNames if (name not in self.excludedColNames)]
        
        # Start select cache thread
        self.resourcesQueue = Queue.Queue(self.config["selectcachesize"])
        if not self._fillSelectCache(cursor): self.resourcesQueue.put(None)
        t = threading.Thread(target = self._selectCacheThread)
        t.daemon = True
        t.start()
        
        cursor.close()
        connection.close()
        
    def _extractConfig(self, configurationsDictionary):
        BasePersistenceHandler._extractConfig(self, configurationsDictionary)
        if ("selectcachesize" not in self.config): raise KeyError("Parameter 'selectcachesize' must be specified.")
        if ("onduplicateupdate" not in self.config): self.config["onduplicateupdate"] = False
        else: self.config["onduplicateupdate"] = common.str2bool(self.config["onduplicateupdate"])
        
    def _fillSelectCache(self, cursor):
        query = "SELECT " + self.config["primarykeycolumn"] + " FROM " + self.config["table"] + " WHERE " + self.config["statuscolumn"] + " = %s ORDER BY " + self.config["primarykeycolumn"] + " LIMIT " + self.config["selectcachesize"] 
        
        self.resourcesQueue.join()
        self.echo.out("Select cache empty. Querying database...")
        cursor.execute(query, (self.status.AVAILABLE,))
        resourcesKeys = cursor.fetchall()
        if resourcesKeys: 
            self.selectEmptyEvent.clear()
            self.echo.out("Filling cache with resources keys...")
            for key in resourcesKeys: self.resourcesQueue.put(key[0])
            self.echo.out("Done.")
            return True
        else: 
            self.selectEmptyEvent.set()
            self.echo.out("No available resource found.")
            return False
        
    def _selectCacheThread(self):
        connection = mysql.connector.connect(**self.config["connargs"])
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            while self._fillSelectCache(cursor): pass
        except: 
            self.selectCacheThreadExceptionEvent.set()
            raise
        finally:
            cursor.close()
            connection.close()
        
    def setup(self):
        self.local.connection = mysql.connector.connect(**self.config["connargs"])
        self.local.connection.autocommit = True
        
    def select(self):
        # Try to get resource key from select cache
        while True:
            try: 
                resourceKey = self.resourcesQueue.get(False, 60)
                if resourceKey: break
            except Queue.Empty:
                if self.selectEmptyEvent.is_set(): 
                    return (None, None, None)
                elif self.selectCacheThreadExceptionEvent.is_set(): 
                    raise RuntimeError("Exception on select cache thread. Execution of MySQLPersistenceHandler aborted.")

        # Fetch resource information and mark it as being processed
        cursor = self.local.connection.cursor(dictionary = True)
        query = "UPDATE " + self.config["table"] + " SET " + self.config["statuscolumn"] + " = %s WHERE " + self.config["primarykeycolumn"] + " = %s"
        cursor.execute(query, (self.status.INPROGRESS, resourceKey))
        self.resourcesQueue.task_done()
        query = "SELECT * FROM " + self.config["table"] + " WHERE " + self.config["primarykeycolumn"] + " = %s"
        cursor.execute(query, (resourceKey,))
        resource = cursor.fetchone()
        cursor.close()
        return (resource[self.config["primarykeycolumn"]], 
                resource[self.config["resourceidcolumn"]], 
                {k: resource[k] for k in self.infoColNames})
        
    def update(self, resourceKey, status, resourceInfo):
        cursor = self.local.connection.cursor()
        if (not resourceInfo): 
            query = "UPDATE " + self.config["table"] + " SET " + self.config["statuscolumn"] + " = %s WHERE " + self.config["primarykeycolumn"] + " = %s"
            cursor.execute(query, (status, resourceKey))
        else: 
            info = {k: resourceInfo[k] for k in resourceInfo if (k not in self.excludedColNames)}
            query = "UPDATE " + self.config["table"] + " SET " + self.config["statuscolumn"] + " = %s, " + " = %s, ".join(info.keys()) + " = %s WHERE " + self.config["primarykeycolumn"] + " = %s"
            cursor.execute(query, (status,) + tuple(info.values()) + (resourceKey,))
        cursor.close()
        
    def insert(self, resourcesList):
        # The method cursor.executemany() is optimized for multiple inserts, batching all data into a single INSERT INTO
        # statement. This method would be the best to use here but unfortunately it does not parse the DEFAULT keyword 
        # correctly. This way, the alternative is to pre-build the query and send it to cursor.execute() instead.
    
        cursor = self.local.connection.cursor()
        query = "INSERT INTO " + self.config["table"] + " (" + ", ".join(self.colNames) + ") VALUES "
        
        data = []
        values = []
        for resourceID, resourceInfo in resourcesList: 
            newResource = {self.config["resourceidcolumn"]: resourceID}
            newResource.update(resourceInfo)
            resourceValues = []
            for column in self.colNames:
                if (column in newResource): 
                    resourceValues.append("%s")
                    data.append(newResource[column])
                else: resourceValues.append("DEFAULT")
            values.append("(" + ", ".join(resourceValues) + ")") 
            
        query += ", ".join(values)
        if (self.config["onduplicateupdate"]):
            query += " ON DUPLICATE KEY UPDATE " + ", ".join(["{0} = VALUES({0})".format(column) for column in self.infoColNames])
        
        cursor.execute(query, data)
        cursor.close()
        
    def count(self):
        cursor = self.local.connection.cursor()
        query = "SELECT " + self.config["statuscolumn"] + ", count(*) FROM " + self.config["table"] + " GROUP BY " + self.config["statuscolumn"]
        
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        
        counts = [0, 0, 0, 0, 0, 0]
        for row in result:
            if (row[0] == self.status.SUCCEEDED): counts[1] = row[1]
            elif (row[0] == self.status.INPROGRESS): counts[2] = row[1]
            elif (row[0] == self.status.AVAILABLE): counts[3] = row[1]
            elif (row[0] == self.status.FAILED): counts[4] = row[1]
            elif (row[0] == self.status.ERROR): counts[5] = row[1]
            counts[0] += row[1]
        
        return tuple(counts)
        
    def reset(self, status):
        cursor = self.local.connection.cursor()
        query = "UPDATE " + self.config["table"] + " SET " + self.config["statuscolumn"] + " = %s WHERE " + self.config["statuscolumn"] + " = %s"
        cursor.execute(query, (self.status.AVAILABLE, status))
        affectedRows = cursor.rowcount
        cursor.close()
        
        # Clear select cache, so reseted resources are crawled as soon as possible
        while not self.resourcesQueue.empty():
            try: 
                self.resourcesQueue.get_nowait()
                self.resourcesQueue.task_done()
            except Queue.Empty: continue
        
        return affectedRows
        
    def finish(self):
        self.local.connection.close()
        