# -*- coding: iso-8859-1 -*-

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
from datetime import datetime
from copy import deepcopy
from collections import deque


class StatusCodes():
    SUCCEEDED  =  2
    INPROGRESS =  1
    AVAILABLE  =  0 
    FAILED     = -1
    ERROR      = -2


class BasePersistenceHandler():  
    def __init__(self, configurationsDictionary): # Receives a copy of everything in the handler section of the XML configuration file as the parameter configurationsDictionary
        self.config = configurationsDictionary
        self.status = StatusCodes() 
    def setup(self): pass # Called when a connection to a client is opened
    def select(self): return (None, None, None) # Returns a tuple: (resource unique key, resource id, resource info dictionary)
    def update(self, resourceKey, status, resourceInfo): pass
    def insert(self, resourcesList): pass # Receives a list of tuples: [(resource id, resource info dictionary), ...]
    def count(self): return (0, 0, 0, 0, 0, 0) # Returns a tuple: (total, succeeded, inprogress, available, failed, error)
    def reset(self, status): return 0 # Returns the number of resources reseted
    def finish(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources
        
        
# This class was built as basis for FilePersistenceHandler and for test purposes. 
# It is not intended for direct use in a production enviroment
class MemoryPersistenceHandler(BasePersistenceHandler):
    def __init__(self, configurationsDictionary): 
        BasePersistenceHandler.__init__(self, configurationsDictionary)
        self._extractConfig(configurationsDictionary)
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
    class GenericFileTypeColumns():
        def __init__(self, fileName, idColumn, statusColumn):
            self.names = self._extractColNames(fileName)
            self.idName = idColumn
            self.statusName = statusColumn
            self.infoNames = [name for name in self.names if (name not in (self.idName, self.statusName))]
        # This method must be overriden to extract the column names for the specific file type
        def _extractColNames(self, fileName): pass 

    class GenericFileTypeHandler():
        # Resource internal representation format: {"id": X, "status": X, "info": {...}}
        def parse(self, resource, columns): pass # Transform resource from file format to internal representation format
        def unparse(self, resource, columns): pass # Transform resource from internal representation format to file format
        def load(self, file, columns): yield None # Generator that yields resources in the internal representation format 
        def dump(self, resources, file, columns): pass # Save a list of resources in internal representation format to file
        
    class JSONColumns(GenericFileTypeColumns):
        def _extractColNames(self, fileName):
            with open(fileName, "r") as file: content = file.read(1024)
            columnsStart = content.index("[") + 1
            columnsEnd = content.index("]")
            columns = content[columnsStart:columnsEnd]
            return [name.strip("\" ") for name in columns.split(",")]
       
    class JSONHandler(GenericFileTypeHandler):    
        def parse(self, resource, columns):
            parsed = {"id": resource[columns.idName]}
            if ((columns.statusName in columns.names) and (columns.statusName in resource)): 
                parsed["status"] = resource[columns.statusName]
            else: parsed["status"] = 0
            if (columns.infoNames):
                parsed["info"] = {}
                for column in columns.infoNames:
                    if (column in resource): parsed["info"][column] = resource[column]
                    else: parsed["info"][column] = None
            return parsed
        
        def unparse(self, resource, columns):
            unparsed = {columns.idName: resource["id"]}
            if (resource["status"] != 0): unparsed[columns.statusName] = resource["status"]
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
            
    class CSVColumns(GenericFileTypeColumns):
        def _extractColNames(self, fileName):
            with open(fileName, "r") as file: 
                reader = csv.DictReader(file, quoting = csv.QUOTE_NONE, skipinitialspace = True)
                columns = reader.fieldnames
            return columns
    
    class CSVHandler(GenericFileTypeHandler):
        def _parseValue(self, value):
            if (not value): return None
            if (not value.startswith("\"")):
                if value.lower() in ("true", "t"): return True
                if value.lower() in ("false", "f"): return False       
                if value.lower() in ("none", "null"): return None
                if ("." in value): return float(value)
                return int(value)
            return value.strip("\"") 
        
        def _unparseValue(self, value):
            if isinstance(value, basestring): return "".join(("\"", value, "\""))
            if isinstance(value, bool): return ("T" if (value) else "F")
            return value
            
        def parse(self, resource, columns):
            parsed = {"id": self._parseValue(resource[columns.idName])}
            if ((columns.statusName in columns.names) and (resource[columns.statusName])): 
                parsed["status"] = self._parseValue(resource[columns.statusName])
            else: parsed["status"] = 0
            if (columns.infoNames):
                parsed["info"] = {}
                for column in columns.infoNames:
                    parsed["info"][column] = self._parseValue(resource[column])
            return parsed
        
        def unparse(self, resource, columns):
            buffer = cStringIO.StringIO()
            writer = csv.DictWriter(buffer, columns.names, quoting = csv.QUOTE_NONE, escapechar = "", quotechar = "", lineterminator = "\n", extrasaction = "ignore")
            unparsed = {columns.idName: self._unparseValue(resource["id"])}
            if (resource["status"] != 0): unparsed[columns.statusName] = self._unparseValue(resource["status"])
            if (resource["info"]):
                for key, value in resource["info"].iteritems():
                    if (value is not None) and (key in columns.infoNames): unparsed[key] = self._unparseValue(value)
            writer.writerow(unparsed)
            return buffer.getvalue()
            
        def load(self, file, columns):
            reader = csv.DictReader(file, quoting = csv.QUOTE_NONE, skipinitialspace = True)
            for resource in reader:
                yield self.parse(resource, columns)
        
        def dump(self, resources, file, columns):
            writer = csv.DictWriter(file, columns.names, quoting = csv.QUOTE_NONE, escapechar = "", quotechar = "", lineterminator = "\n", extrasaction = "ignore")
            writer.writeheader()
            # In case of CSV, it is easier and faster to unparse the resource here, 
            # so we can use writerow method to directly save the resource to file
            for resource in resources:
                row = {columns.idName: self._unparseValue(resource["id"])}
                if (resource["status"] != 0): row[columns.statusName] = self._unparseValue(resource["status"])
                if (resource["info"]):
                    for key, value in resource["info"].iteritems():
                        if (value is not None) and (key in columns.infoNames): row[key] = self._unparseValue(value)
                writer.writerow(row)

    def __init__(self, configurationsDictionary): 
        MemoryPersistenceHandler.__init__(self, configurationsDictionary)
        self.echo = common.EchoHandler()
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
        if (self.config["savetimedelta"] < 1): raise ValueError("Parameter savetimedelta must be greater than zero.")
    
    def _save(self, pk, id, status, info, changeInfo = True):
        with self.saveLock: MemoryPersistenceHandler._save(self, pk, id, status, info, changeInfo)
    
    # Define internal file handler based on file type. Change this function to add support to other file types
    def _setFileHandler(self):
        if (self.config["filetype"] == "json"): 
            self.fileColumns = self.JSONColumns(self.config["filename"], self.config["resourceidcolumn"], self.config["statuscolumn"])
            self.fileHandler = self.JSONHandler()
        elif (self.config["filetype"] == "csv"): 
            self.fileColumns = self.CSVColumns(self.config["filename"], self.config["resourceidcolumn"], self.config["statuscolumn"])
            self.fileHandler = self.CSVHandler()
        else: 
            raise TypeError("Unknown file type '%s'." % self.config["filetype"])
            
    def _checkDumpException(function):
        def decoratedFunction(self, *args):
            if (self.dumpExceptionEvent.is_set()): 
                raise RuntimeError("Exception on dump thread. Execution of file persistence handler aborted.")
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
        except Except as error:
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
    def __init__(self, configurationsDictionary): 
        self.originalConfig = deepcopy(configurationsDictionary)
        MemoryPersistenceHandler.__init__(self, configurationsDictionary)
        self.echo = common.EchoHandler()
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
        
        if (self.config["sizethreshold"] < 0): raise ValueError("Parameter sizethreshold must be zero or greater.")
        if (self.config["amountthreshold"] < 0): raise ValueError("Parameter amountthreshold must be zero or greater.")
        if (self.config["sizethreshold"] == 0) and (self.config["amountthreshold"] == 0): 
            raise ValueError("Parameters sizethreshold and amountthreshold cannot be zero at the same time.")
            
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
                except KeyError as error: raise KeyError("Cannot insert resource, ID %s already exists in '%s'." % (resourceID, handler.config["filename"]))
                continue
        
            with self.insertLock:
                handler = self.fileHandlersList[self.insertHandlerIndex]
      
                # If size or amount thresholds were exceeded, change insert handler. If there is no more
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
    def __init__(self, configurationsDictionary):
        BasePersistenceHandler.__init__(self, configurationsDictionary)
        self._extractConfig(configurationsDictionary)
        self.local = threading.local()
        
        # Get column names
        connection = mysql.connector.connect(user=self.config["user"], password=self.config["password"], host=self.config["host"], database=self.config["name"])
        cursor = connection.cursor()
        query = "SELECT * FROM " + self.config["table"] + " LIMIT 0"
        cursor.execute(query)
        cursor.fetchall()
        self.colNames = cursor.column_names
        self.excludedColNames = (self.config["primarykeycolumn"], self.config["resourceidcolumn"], self.config["statuscolumn"])
        self.infoColNames = [name for name in self.colNames if (name not in self.excludedColNames)]
        cursor.close()
        connection.close()
        
    def _extractConfig(self, configurationsDictionary):
        if ("onduplicateupdate" not in self.config): self.config["onduplicateupdate"] = False
        else: self.config["onduplicateupdate"] = common.str2bool(self.config["onduplicateupdate"])
        
    def setup(self):
        self.local.connection = mysql.connector.connect(user=self.config["user"], password=self.config["password"], host=self.config["host"], database=self.config["name"])
        self.local.lastSelectID = None
        
    def select(self):
        cursor = self.local.connection.cursor(dictionary = True)
        query = "UPDATE " + self.config["table"] + " SET " + self.config["primarykeycolumn"] + " = LAST_INSERT_ID(" + self.config["primarykeycolumn"] + "), " + self.config["statuscolumn"] + " = %s WHERE " + self.config["statuscolumn"] + " = %s ORDER BY " + self.config["primarykeycolumn"] + " LIMIT 1"
        cursor.execute(query, (self.status.INPROGRESS, self.status.AVAILABLE))
        query = "SELECT * FROM " + self.config["table"] + " WHERE " + self.config["primarykeycolumn"] + " = LAST_INSERT_ID()"
        cursor.execute(query)
        resource = cursor.fetchone()
        self.local.connection.commit()
        cursor.close()
        if (resource) and (resource[self.config["primarykeycolumn"]] != self.local.lastSelectID): 
            self.local.lastSelectID = resource[self.config["primarykeycolumn"]]
            return (resource[self.config["primarykeycolumn"]], 
                    resource[self.config["resourceidcolumn"]], 
                    {k: resource[k] for k in self.infoColNames})
        else: return (None, None, None)
        
    def update(self, resourceKey, status, resourceInfo):
        cursor = self.local.connection.cursor()
        if (not resourceInfo): 
            query = "UPDATE " + self.config["table"] + " SET " + self.config["statuscolumn"] + " = %s WHERE " + self.config["primarykeycolumn"] + " = %s"
            cursor.execute(query, (status, resourceKey))
        else: 
            info = {k: resourceInfo[k] for k in resourceInfo if (k not in self.excludedColNames)}
            query = "UPDATE " + self.config["table"] + " SET " + self.config["statuscolumn"] + " = %s, " + " = %s, ".join(info.keys()) + " = %s WHERE " + self.config["primarykeycolumn"] + " = %s"
            cursor.execute(query, (status,) + tuple(info.values()) + (resourceKey,))
        self.local.connection.commit()
        cursor.close()
        
    def insert(self, resourcesList):
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
        self.local.connection.commit()        
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
        self.local.connection.commit()
        cursor.close()
        return affectedRows
        
    def finish(self):
        self.local.connection.close()
        