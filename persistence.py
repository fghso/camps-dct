# -*- coding: iso-8859-1 -*-

import os
import threading
import json
import csv
import shutil
import common
import mysql.connector
from datetime import datetime
from copy import deepcopy
from collections import deque
from collections import OrderedDict


class StatusCodes():
    SUCCEEDED   =  2
    INPROGRESS =  1
    AVAILABLE  =  0 
    FAILED     = -1
    ERROR      = -2


class BasePersistenceHandler():  
    status = StatusCodes()
    def __init__(self, configurationsDictionary): pass # Receives a copy of everything in the handler section of the XML configuration file as the parameter configurationsDictionary
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
    loadLock = threading.Lock()
    insertLock = threading.Lock()
    duplicatedIDException = None
    resources = []
    insertedResources = []
    IDsHash = {}
    statusRecords = {BasePersistenceHandler.status.SUCCEEDED:  [],
                     BasePersistenceHandler.status.INPROGRESS: [],
                     BasePersistenceHandler.status.AVAILABLE:  deque(), 
                     BasePersistenceHandler.status.FAILED:     [],
                     BasePersistenceHandler.status.ERROR:      []}

    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        with self.loadLock:
            if (MemoryPersistenceHandler.duplicatedIDException is not None): 
                raise MemoryPersistenceHandler.duplicatedIDException
            if (not self.resources):
                # Some data for tests
                self.resources.extend([
                    {"id": 1, "status": 0, "info": {"crawler_name": "c1", "response_code": 3}},
                    {"id": 3, "status": 0, "info": {"crawler_name": "c2", "response_code": 3}},
                    {"id": 3, "status": 0, "info": None},
                    {"id": 4, "status": 0, "info": None}
                ])
                for pk, resource in enumerate(self.resources):
                    self.statusRecords[resource["status"]].append(pk)
                    if (self.config["uniqueresourceid"]): 
                        if (resource["id"] not in self.IDsHash): 
                            self.IDsHash[resource["id"]] = (self.resources, pk)
                        else: 
                            MemoryPersistenceHandler.duplicatedIDException = KeyError("Duplicated ID found in resources list: %s." % resource["id"])
                            raise MemoryPersistenceHandler.duplicatedIDException
            
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        
        if ("uniqueresourceid" not in self.config): self.config["uniqueresourceid"] = False
        else: self.config["uniqueresourceid"] = common.str2bool(self.config["uniqueresourceid"])
    
        if ("ondupkeyupdate" not in self.config): self.config["ondupkeyupdate"] = False
        else: self.config["ondupkeyupdate"] = common.str2bool(self.config["ondupkeyupdate"])
        
        if ("separateinsertlist" not in self.config): self.config["separateinsertlist"] = False
        else: self.config["separateinsertlist"] = common.str2bool(self.config["separateinsertlist"])
        
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
        insertList = []
        if (self.config["separateinsertlist"]): insertList = self.insertedResources
        else: insertList = self.resources
        for resourceID, resourceInfo in resourcesList:
            if (self.config["uniqueresourceid"]) and (resourceID in self.IDsHash):
                updateList, pk = self.IDsHash[resourceID]
                if (self.config["ondupkeyupdate"]): 
                    self._save(updateList, pk, None, None, resourceInfo)
                    continue
                else: raise KeyError("Cannot insert resource, ID %s already exists." % resourceID)
            with self.insertLock:
                if (not self.config["separateinsertlist"]): self.statusRecords[self.status.AVAILABLE].append(len(insertList))
                if (self.config["uniqueresourceid"]): self.IDsHash[resourceID] = (insertList, len(insertList))
                self._save(insertList, None, resourceID, self.status.AVAILABLE, resourceInfo)
        
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
        def __init__(self, idColumnName, statusColumnName):
            self.selectColNames = None
            self.idColumn = idColumnName
            self.statusColumn = statusColumnName
            self.selectExcludeColNames = (self.idColumn, self.statusColumn)
            self.insertExcludeColNames = (self.idColumn,)
        def getFileColNames(self, file): return [] # Returns the column names specified in the file as a sequence (list or tuple) or None if no column name could be found
        def load(self, file): yield None # Generator that yields resources in the format {"id": X, "status": X, "info": {...}}
        def dump(self, resourcesList, columnNames, file): pass
       
    class JSONHandler(GenericFileTypeHandler):    
        def getFileColNames(self, file):
            input = None
            try: input = json.load(file)["columns"]
            except (ValueError, KeyError): pass
            return input
            
        def load(self, file): 
            input = json.load(file)
            self.selectColNames = input["columns"]
            selectInfoColNames = [name for name in self.selectColNames if (name not in self.selectExcludeColNames)]
            for element in input["resources"]: 
                resource = {"id": element[self.idColumn]}
                if (self.statusColumn in element): resource["status"] = element[self.statusColumn]
                else: resource["status"] = 0
                if (selectInfoColNames):
                    resource["info"] = {}
                    for column in selectInfoColNames:
                        if (column in element): resource["info"][column] = element[column]
                        else: resource["info"][column] = None
                yield resource

        def dump(self, resourcesList, columnNames, file):
            file.write("{\"columns\": %s, \"resources\": [" % json.dumps(columnNames))
            insertInfoColNames = [name for name in columnNames if (name not in self.insertExcludeColNames)]
            separator = ""
            for resource in resourcesList:
                element = {self.idColumn: resource["id"]}
                if (resource["status"] != 0): element[self.statusColumn] = resource["status"]
                if (resource["info"]): 
                    for key, value in resource["info"].iteritems(): 
                        if (value) and (key in insertInfoColNames): element[key] = value
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
            
        def getFileColNames(self, file):
            reader = csv.DictReader(file, quoting = csv.QUOTE_NONE)
            return reader.fieldnames
        
        def load(self, file):
            reader = csv.DictReader(file, quoting = csv.QUOTE_NONE)
            self.selectColNames = reader.fieldnames
            selectInfoColNames = [name for name in self.selectColNames if (name not in self.selectExcludeColNames)]
            for row in reader:
                resource = {"id": self._csvParseValue(row[self.idColumn])}
                if (row[self.statusColumn]): 
                    resource["status"] = self._csvParseValue(row[self.statusColumn])
                else: resource["status"] = 0
                if (selectInfoColNames):
                    resource["info"] = {}
                    for column in selectInfoColNames:
                        resource["info"][column] = self._csvParseValue(row[column])
                yield resource
        
        def dump(self, resourcesList, columnNames, file):
            writer = csv.DictWriter(file, columnNames, quoting = csv.QUOTE_NONE, escapechar = "", quotechar = "", lineterminator = "\n", extrasaction = "ignore")
            writer.writeheader()
            insertInfoColNames = [name for name in columnNames if (name not in self.insertExcludeColNames)]
            for resource in resourcesList:
                row = {self.idColumn: self._csvUnparseValue(resource["id"])}
                if (resource["status"] != 0): row[self.statusColumn] = self._csvUnparseValue(resource["status"])
                if (resource["info"]):
                    for key, value in resource["info"].iteritems():
                        if (value) and (key in insertInfoColNames): row[key] = self._csvUnparseValue(value)
                writer.writerow(row)
    
    saveLock = threading.Lock()
    lastSaveTime = None
    selectColNames = None
    insertColNames = None

    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        self.echo = common.EchoHandler()
        self._setFileHandlers()
        with self.loadLock:
            if (MemoryPersistenceHandler.duplicatedIDException is not None): 
                raise MemoryPersistenceHandler.duplicatedIDException
            if (not self.resources):
                self.echo.default("Loading resources from disk...")
                file = open(self.selectConfig["filename"], "r")
                try:
                    resourcesList = self.selectHandler.load(file)
                    for resource in resourcesList:
                        self.statusRecords[resource["status"]].append(len(self.resources))
                        if (self.config["uniqueresourceid"]): 
                            if (resource["id"] not in self.IDsHash): 
                                self.IDsHash[resource["id"]] = (self.resources, len(self.resources))
                            else: 
                                MemoryPersistenceHandler.duplicatedIDException = KeyError("Duplicated ID found in resources list: %s." % resource["id"])
                                raise MemoryPersistenceHandler.duplicatedIDException
                        if ("info" not in resource): resource["info"] = None
                        self.resources.append(resource)
                except: raise
                finally: file.close()
                self.echo.default("Done.")
                FilePersistenceHandler.lastSaveTime = datetime.now()
                FilePersistenceHandler.selectColNames = self.selectHandler.selectColNames
                self._setInsertColNames()
            
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        self.selectConfig = configurationsDictionary["select"]
        if ("insert" not in configurationsDictionary): self.insertConfig = self.selectConfig
        else: self.insertConfig = configurationsDictionary["insert"]
        
        if ("uniqueresourceid" not in self.config): self.config["uniqueresourceid"] = False
        else: self.config["uniqueresourceid"] = common.str2bool(self.config["uniqueresourceid"])
    
        if ("ondupkeyupdate" not in self.config): self.config["ondupkeyupdate"] = False
        else: self.config["ondupkeyupdate"] = common.str2bool(self.config["ondupkeyupdate"])
        
        self.config["savetimedelta"] = int(self.config["savetimedelta"])
        if (self.config["savetimedelta"] < 1): raise ValueError("Parameter savetimedelta must be greater than 1 second.")
        
        if (self.insertConfig["filename"] == self.selectConfig["filename"]): self.config["separateinsertlist"] = False
        else: self.config["separateinsertlist"] = True
        
        if ("resourceidcolumn" not in self.insertConfig): 
            self.insertConfig["resourceidcolumn"] = self.selectConfig["resourceidcolumn"]
        if ("statuscolumn" not in self.insertConfig): 
            self.insertConfig["statuscolumn"] = self.selectConfig["statuscolumn"]
    
    # Define internal file handler based on file type. Change this function to add support to other file types
    def _setFileHandlers(self):
        selectIDColumn = self.selectConfig["resourceidcolumn"]
        selectStatusColumn = self.selectConfig["statuscolumn"]
        insertIDColumn = self.insertConfig["resourceidcolumn"]
        insertStatusColumn = self.insertConfig["statuscolumn"]
    
        # Extract file types based on file extensions
        selectFileType = os.path.splitext(self.selectConfig["filename"])[1][1:].lower()
        insertFileType = os.path.splitext(self.insertConfig["filename"])[1][1:].lower()
        
        # Select handler
        if (selectFileType == "json"): self.selectHandler = self.JSONHandler(selectIDColumn, selectStatusColumn)
        elif (selectFileType == "csv"): self.selectHandler = self.CSVHandler(selectIDColumn, selectStatusColumn)
        else: raise TypeError("Unknown file type '%s'." % self.selectConfig["filename"])
        
        # Insert handler
        if (insertFileType == "json"): self.insertHandler = self.JSONHandler(insertIDColumn, insertStatusColumn)
        elif (insertFileType == "csv"): self.insertHandler = self.CSVHandler(insertIDColumn, insertStatusColumn)
        else: raise TypeError("Unknown file type '%s'." % self.insertConfig["filename"])
            
    def _setInsertColNames(self):
        if (self.config["separateinsertlist"]): 
            try:
                with open(self.insertConfig["filename"], "r") as file: 
                    FilePersistenceHandler.insertColNames = self.insertHandler.getFileColNames(file)
            except IOError: 
                with open(self.insertConfig["filename"], "w"): pass
        if (not FilePersistenceHandler.insertColNames): 
            FilePersistenceHandler.insertColNames = FilePersistenceHandler.selectColNames
            
    def _save(self, list, pk, id, status, info, changeInfo = True):
        with self.saveLock: MemoryPersistenceHandler._save(self, list, pk, id, status, info, changeInfo)
        
    def _checkTimeDelta(self):
        with self.saveLock:
            elapsedTime = datetime.now() - FilePersistenceHandler.lastSaveTime
            if (elapsedTime.seconds >= self.config["savetimedelta"]):
                self._dump()
                FilePersistenceHandler.lastSaveTime = datetime.now()
        
    def _dump(self):
        self.echo.default("Saving list of resources to disk...")
        with open("dump.temp", "w") as tempFile: 
            self.selectHandler.dump(self.resources, self.selectColNames, tempFile)
        try: os.rename("dump.temp", self.selectConfig["filename"])
        except WindowsError: 
            os.remove(self.selectConfig["filename"])
            os.rename("dump.temp", self.selectConfig["filename"])
        if (self.insertedResources): 
            self.echo.default("Saving list of inserted resources to disk...")
            with open(self.insertConfig["filename"], "r") as originalFile:
                with open("dump.temp", "w") as tempFile: 
                    shutil.copyfileobj(originalFile, tempFile, 10485760)
                    self.insertHandler.dump(self.insertedResources, self.insertColNames, tempFile)
            try: os.rename("dump.temp", self.insertConfig["filename"])
            except WindowsError: 
                os.remove(self.insertConfig["filename"])
                os.rename("dump.temp", self.insertConfig["filename"])
        self.echo.default("Done.")
                
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
        self._extractConfig(configurationsDictionary)
        self.lastSelectID = None
        
        # Select connection
        self.mysqlSelectConnection = mysql.connector.connect(user=self.selectConfig["user"], password=self.selectConfig["password"], host=self.selectConfig["host"], database=self.selectConfig["name"])
        self.selectColNames = self._getColNames(self.mysqlSelectConnection, self.selectConfig["table"])
        self.selectExcludeColNames = (self.selectConfig["primarykeycolumn"], self.selectConfig["resourceidcolumn"], self.selectConfig["statuscolumn"])
        self.selectInfoColNames = [name for name in self.selectColNames if (name not in self.selectExcludeColNames)]
        
        # Insert connection
        if ((self.insertConfig["host"] != self.selectConfig["host"]) 
            or (self.insertConfig["name"] != self.selectConfig["name"])):
            self.mysqlInsertConnection = mysql.connector.connect(user=self.insertConfig["user"], password=self.insertConfig["password"], host=self.insertConfig["host"], database=self.insertConfig["name"])
            colNames = self._getColNames(self.mysqlInsertConnection, self.insertConfig["table"])
            self.insertColNames = [name for name in colNames if (name != self.insertConfig["resourceidcolumn"])]
        else: 
            self.mysqlInsertConnection = self.mysqlSelectConnection
            self.insertColNames = [name for name in self.selectColNames if (name != self.insertConfig["resourceidcolumn"])]
        self.insertExcludeColNames = (self.insertConfig["primarykeycolumn"], self.insertConfig["statuscolumn"])
        self.insertInfoColNames = [name for name in self.insertColNames if (name not in self.insertExcludeColNames)]
                
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        self.selectConfig = configurationsDictionary["select"]
        if ("insert" not in configurationsDictionary): self.insertConfig = self.selectConfig
        else: self.insertConfig = configurationsDictionary["insert"]
        
        # Set default values
        if ("ondupkeyupdate" not in self.config): self.config["ondupkeyupdate"] = False
        else: self.config["ondupkeyupdate"] = common.str2bool(self.config["ondupkeyupdate"])
        
        if ("user" not in self.insertConfig): self.insertConfig["user"] = self.selectConfig["user"]
        if ("password" not in self.insertConfig): self.insertConfig["password"] = self.selectConfig["password"]
        if ("host" not in self.insertConfig): self.insertConfig["host"] = self.selectConfig["host"]
        if ("name" not in self.insertConfig): self.insertConfig["name"] = self.selectConfig["name"]
        if ("table" not in self.insertConfig): self.insertConfig["table"] = self.selectConfig["table"]
        if ("primarykeycolumn" not in self.insertConfig): 
            self.insertConfig["primarykeycolumn"] = self.selectConfig["primarykeycolumn"]
        if ("resourceidcolumn" not in self.insertConfig): 
            self.insertConfig["resourceidcolumn"] = self.selectConfig["resourceidcolumn"]
        if ("statuscolumn" not in self.insertConfig): 
            self.insertConfig["statuscolumn"] = self.selectConfig["statuscolumn"]
        
    def _getColNames(self, mysqlConnection, tableName):
        cursor = mysqlConnection.cursor()
        query = "SELECT * FROM " + tableName + " LIMIT 0"
        cursor.execute(query)
        cursor.fetchall()
        colNames = cursor.column_names
        cursor.close()
        return colNames
        
    def select(self):
        cursor = self.mysqlSelectConnection.cursor(dictionary = True)
        query = "UPDATE " + self.selectConfig["table"] + " SET " + self.selectConfig["primarykeycolumn"] + " = LAST_INSERT_ID(" + self.selectConfig["primarykeycolumn"] + "), " + self.selectConfig["statuscolumn"] + " = %s WHERE " + self.selectConfig["statuscolumn"] + " = %s ORDER BY " + self.selectConfig["primarykeycolumn"] + " LIMIT 1"
        cursor.execute(query, (self.status.INPROGRESS, self.status.AVAILABLE))
        query = "SELECT * FROM " + self.selectConfig["table"] + " WHERE " + self.selectConfig["primarykeycolumn"] + " = LAST_INSERT_ID()"
        cursor.execute(query)
        resource = cursor.fetchone()
        self.mysqlSelectConnection.commit()
        cursor.close()
        if (resource) and (resource[self.selectConfig["primarykeycolumn"]] != self.lastSelectID): 
            self.lastSelectID = resource[self.selectConfig["primarykeycolumn"]]
            return (resource[self.selectConfig["primarykeycolumn"]], 
                    resource[self.selectConfig["resourceidcolumn"]], 
                    {k: resource[k] for k in self.selectInfoColNames})
        else: return (None, None, None)
        
    def update(self, resourceKey, status, resourceInfo):
        cursor = self.mysqlSelectConnection.cursor()
        if (not resourceInfo): 
            query = "UPDATE " + self.selectConfig["table"] + " SET " + self.selectConfig["statuscolumn"] + " = %s WHERE " + self.selectConfig["primarykeycolumn"] + " = %s"
            cursor.execute(query, (status, resourceKey))
        else: 
            info = {k: resourceInfo[k] for k in resourceInfo if (k not in self.selectExcludeColNames)}
            query = "UPDATE " + self.selectConfig["table"] + " SET " + self.selectConfig["statuscolumn"] + " = %s, " + " = %s, ".join(info.keys()) + " = %s WHERE " + self.selectConfig["primarykeycolumn"] + " = %s"
            cursor.execute(query, (status,) + tuple(info.values()) + (resourceKey,))
        self.mysqlSelectConnection.commit()
        cursor.close()
        
    def insert(self, resourcesList):
        cursor = self.mysqlInsertConnection.cursor()
        query = "INSERT INTO " + self.insertConfig["table"] + " (" + ", ".join([self.insertConfig["resourceidcolumn"]] + self.insertColNames) + ") VALUES "
        
        data = []
        values = []
        for resourceID, resourceInfo in resourcesList: 
            resourceValues = [str(resourceID)]
            if (not resourceInfo): resourceInfo = {}
            for column in self.insertColNames:
                if (column in resourceInfo): 
                    resourceValues.append("%s")
                    data.append(resourceInfo[column])
                else: resourceValues.append("DEFAULT")
            values.append("(" + ", ".join(resourceValues) + ")") 
            
        query += ", ".join(values)
        if (self.config["ondupkeyupdate"]):
            query += " ON DUPLICATE KEY UPDATE " + ", ".join(["{0} = VALUES({0})".format(column) for column in [self.insertConfig["resourceidcolumn"]] + self.insertInfoColNames])
            
        cursor.execute(query, data)
        self.mysqlInsertConnection.commit()        
        cursor.close()
        
    def count(self):
        cursor = self.mysqlSelectConnection.cursor()
        query = "SELECT " + self.selectConfig["statuscolumn"] + ", count(*) FROM " + self.selectConfig["table"] + " GROUP BY " + self.selectConfig["statuscolumn"]
        
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
        cursor = self.mysqlSelectConnection.cursor()
        query = "UPDATE " + self.selectConfig["table"] + " SET " + self.selectConfig["statuscolumn"] + " = %s WHERE " + self.selectConfig["statuscolumn"] + " = %s"
        cursor.execute(query, (self.status.AVAILABLE, status))
        affectedRows = cursor.rowcount
        self.mysqlSelectConnection.commit()
        cursor.close()
        return affectedRows
        
    def finish(self):
        self.mysqlSelectConnection.close()
        