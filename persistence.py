# -*- coding: iso-8859-1 -*-

import os
import threading
import json
import csv
import tempfile
import common
import mysql.connector
from datetime import datetime
from copy import deepcopy
from collections import deque
from collections import OrderedDict


class StatusCodes():
    SUCCEEDED  =  2
    INPROGRESS =  1
    AVAILABLE  =  0 
    FAILED     = -1
    ERROR      = -2


class BasePersistenceHandler():  
    globalIDsHash = {}
    def __init__(self, configurationsDictionary): self.status = StatusCodes() # Receives a copy of everything in the handler section of the XML configuration file as the parameter configurationsDictionary
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
            
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        
        if ("uniqueresourceid" not in self.config): self.config["uniqueresourceid"] = False
        else: self.config["uniqueresourceid"] = common.str2bool(self.config["uniqueresourceid"])
    
        if ("onduplicateupdate" not in self.config): self.config["onduplicateupdate"] = False
        else: self.config["onduplicateupdate"] = common.str2bool(self.config["onduplicateupdate"])
        
    def _save(self, list, pk, id, status, info, changeInfo = True):
        if (pk is not None):
            if (status is not None): list[pk]["status"] = status
            if (changeInfo): 
                if (list[pk]["info"] is not None) and (info is not None): list[pk]["info"].update(info)
                else: list[pk]["info"] = info
        else: 
            list.append({"id": id, "status": status, "info": info})
    
    def select(self): 
        try: pk = self.statusRecords[self.status.AVAILABLE].popleft()
        except IndexError: return (None, None, None)
        self._save(self.resources, pk, None, self.status.INPROGRESS, None, False)
        self.statusRecords[self.status.INPROGRESS].append(pk)
        return (pk, self.resources[pk]["id"], deepcopy(self.resources[pk]["info"]))
    
    def update(self, resourceKey, status, resourceInfo): 
        currentStatus = self.resources[resourceKey]["status"]
        self.statusRecords[currentStatus].remove(resourceKey)
        if (resourceInfo): self._save(self.resources, resourceKey, None, status, resourceInfo)
        else: self._save(self.resources, resourceKey, None, status, resourceInfo, False)
        self.statusRecords[status].append(resourceKey)
        
    def insert(self, resourcesList): 
        for resourceID, resourceInfo in resourcesList:
            if (self.config["uniqueresourceid"]) and (resourceID in self.IDsHash):
                if (self.config["onduplicateupdate"]): 
                    self._save(self.resources, self.IDsHash[resourceID], None, None, resourceInfo)
                    continue
                else: raise KeyError("Cannot insert resource, ID %s already exists." % resourceID)
            with self.insertLock:
                self.statusRecords[self.status.AVAILABLE].append(len(self.resources))
                if (self.config["uniqueresourceid"]): self.IDsHash[resourceID] = len(self.resources)
                self._save(self.resources, None, resourceID, self.status.AVAILABLE, resourceInfo)
        
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
            self._save(self.resources, pk, None, self.status.AVAILABLE, None, False)
            self.statusRecords[self.status.AVAILABLE].appendleft(pk)
        return len(resetList)
            
        
class FilePersistenceHandler(MemoryPersistenceHandler):
    class GenericFileTypeHandler():
        def __init__(self, idColumn, statusColumn):
            self.idColumn = idColumn
            self.statusColumn = statusColumn
        def load(self, file): yield None # Generator that yields resources in the format {"id": X, "status": X, "info": {...}}
        def dump(self, resources, file): pass
       
    class JSONHandler(GenericFileTypeHandler):    
        def load(self, file): 
            input = json.load(file)
            self.colNames = input["columns"]
            self.infoColNames = [name for name in self.colNames if (name not in (self.idColumn, self.statusColumn))]
            for element in input["resources"]: 
                resource = {"id": element[self.idColumn]}
                if ((self.statusColumn in self.colNames) and (self.statusColumn in element)): 
                    resource["status"] = element[self.statusColumn]
                else: resource["status"] = 0
                if (self.infoColNames):
                    resource["info"] = {}
                    for column in self.infoColNames:
                        if (column in element): resource["info"][column] = element[column]
                        else: resource["info"][column] = None
                yield resource

        def dump(self, resources, file):
            file.write("{\"columns\": %s, \"resources\": [" % json.dumps(self.colNames))
            separator = ""
            for resource in resources:
                element = {self.idColumn: resource["id"]}
                if (resource["status"] != 0): element[self.statusColumn] = resource["status"]
                if (resource["info"]): 
                    for key, value in resource["info"].iteritems(): 
                        if (value is not None) and (key in self.infoColNames): element[key] = value
                file.write("%s%s" % (separator, json.dumps(element)))
                separator = ", "
            file.write("]}")
    
    class CSVHandler(GenericFileTypeHandler):
        def _csvParseValue(self, value):
            if (not value): return None
            if (not value.startswith("\"")):
                if value.lower() in ("true", "t"): return True
                if value.lower() in ("false", "f"): return False       
                if value.lower() in ("none", "null"): return None
                if ("." in value): return float(value)
                return int(value)
            return value.strip("\"") 
        
        def _csvUnparseValue(self, value):
            if isinstance(value, basestring): return "".join(("\"", value, "\""))
            if isinstance(value, bool): return ("T" if (value) else "F")
            return value
            
        def load(self, file):
            reader = csv.DictReader(file, quoting = csv.QUOTE_NONE, skipinitialspace = True)
            self.colNames = reader.fieldnames
            self.infoColNames = [name for name in self.colNames if (name not in (self.idColumn, self.statusColumn))]
            for row in reader:
                resource = {"id": self._csvParseValue(row[self.idColumn])}
                if ((self.statusColumn in self.colNames) and (row[self.statusColumn])): 
                    resource["status"] = self._csvParseValue(row[self.statusColumn])
                else: resource["status"] = 0
                if (self.infoColNames):
                    resource["info"] = {}
                    for column in self.infoColNames:
                        resource["info"][column] = self._csvParseValue(row[column])
                yield resource
        
        def dump(self, resources, file):
            writer = csv.DictWriter(file, self.colNames, quoting = csv.QUOTE_NONE, escapechar = "", quotechar = "", lineterminator = "\n", extrasaction = "ignore")
            writer.writeheader()
            for resource in resources:
                row = {self.idColumn: self._csvUnparseValue(resource["id"])}
                if (resource["status"] != 0): row[self.statusColumn] = self._csvUnparseValue(resource["status"])
                if (resource["info"]):
                    for key, value in resource["info"].iteritems():
                        if (value is not None) and (key in self.infoColNames): row[key] = self._csvUnparseValue(value)
                writer.writerow(row)

    def __init__(self, configurationsDictionary): 
        MemoryPersistenceHandler.__init__(self, configurationsDictionary)
        self._setFileHandler()
        self.echo = common.EchoHandler()
        self.saveLock = threading.Lock()
        file = open(self.config["filename"], "r")
        try:
            resourcesList = self.fileHandler.load(file)
            for resource in resourcesList:
                self.statusRecords[resource["status"]].append(len(self.resources))
                if (self.config["uniqueresourceid"]): 
                    if (resource["id"] not in self.IDsHash): self.IDsHash[resource["id"]] = len(self.resources)
                    else: raise KeyError("Duplicated ID found in resources list: %s." % resource["id"]) 
                if ("info" not in resource): resource["info"] = None
                self.resources.append(resource)
        except: raise
        finally: file.close()
        self.lastSaveTime = datetime.now()
    
    # Define internal file handler based on file type. Change this function to add support to other file types
    def _setFileHandler(self):
        fileName = self.config["filename"]
        idColumn = self.config["resourceidcolumn"]
        statusColumn = self.config["statuscolumn"]
    
        # Extract file type based on file extension
        fileType = os.path.splitext(fileName)[1][1:].lower()
        
        # Set file handler
        if (fileType == "json"): self.fileHandler = self.JSONHandler(idColumn, statusColumn)
        elif (fileType == "csv"): self.fileHandler = self.CSVHandler(idColumn, statusColumn)
        else: raise TypeError("Unknown file type '%s'." % fileName)
        
    def _checkTimeDelta(self):
        with self.saveLock:
            elapsedTime = datetime.now() - self.lastSaveTime
            if (elapsedTime.seconds >= self.config["savetimedelta"]):
                self._dump()
                self.lastSaveTime = datetime.now()
        
    def _dump(self):
        self.echo.out("Saving list of resources to disk...")
        with tempfile.NamedTemporaryFile(mode = "w", suffix = ".temp", prefix = "dump_", dir = "", delete = False) as temp: 
            self.fileHandler.dump(self.resources, temp)
        common.replace(temp.name, self.config["filename"])
        self.echo.out("Done.")
        
    def _extractConfig(self, configurationsDictionary):
        MemoryPersistenceHandler._extractConfig(self, configurationsDictionary)
        self.config["savetimedelta"] = int(self.config["savetimedelta"])
        if (self.config["savetimedelta"] < 1): raise ValueError("Parameter savetimedelta must be greater than 1 second.")
    
    def _save(self, list, pk, id, status, info, changeInfo = True):
        with self.saveLock: MemoryPersistenceHandler._save(self, list, pk, id, status, info, changeInfo)
                
    def update(self, resourceKey, status, resourceInfo): 
        MemoryPersistenceHandler.update(self, resourceKey, status, resourceInfo)
        self._checkTimeDelta()
        
    def insert(self, resourcesList): 
        MemoryPersistenceHandler.insert(self, resourcesList)
        self._checkTimeDelta()
        
    def reset(self, status):    
        resetedCount = MemoryPersistenceHandler.reset(self, status)    
        self._checkTimeDelta()
        return resetedCount

    def shutdown(self): 
        with self.saveLock: self._dump()
        
        
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
        self.config = configurationsDictionary
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
        