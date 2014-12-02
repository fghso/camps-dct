# -*- coding: iso-8859-1 -*-

import threading
import json
import common
import mysql.connector
from datetime import datetime
from copy import deepcopy
from collections import deque


class BasePersistenceHandler():  
    statusCodes = {"SUCCEDED":   2,
                   "INPROGRESS": 1,
                   "AVAILABLE":  0, 
                   "FAILED":    -1,
                   "ERROR":     -2}

    def __init__(self, configurationsDictionary): pass # Receives a copy of everything in handler section of the XML configuration file
    def select(self): return (None, None, None) # Return a tuple: (resource unique key, resource id, resource info dictionary)
    def update(self, resourceKey, status, resourceInfo): pass
    def insert(self, resourcesList): pass # Receives a list of tuples: [(resource id, resource info dictionary), ...]
    def count(self): return (0, 0, 0, 0, 0, 0) # Return a tuple: (total, succeeded, inprogress, available, failed, error)
    def reset(self, status): return 0 # Return the number of resources reseted
    def close(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources
        
        
# This class was built as basis for FilePersistenceHandler and for test purposes. 
# It is not intended for direct use in a production enviroment
class MemoryPersistenceHandler(BasePersistenceHandler):
    loadLock = threading.Lock()
    insertLock = threading.Lock()
    resources = []
    insertedResources = []
    IDsHash = {}
    statusRecords = {BasePersistenceHandler.statusCodes["SUCCEDED"]:   0,
                     BasePersistenceHandler.statusCodes["INPROGRESS"]: [],
                     BasePersistenceHandler.statusCodes["AVAILABLE"]:  deque(), 
                     BasePersistenceHandler.statusCodes["FAILED"]:     [],
                     BasePersistenceHandler.statusCodes["ERROR"]:      []}

    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        with self.loadLock:
            if (not self.resources):
                # Some data for tests
                self.resources.extend([
                    {"id": 1, "status": 0, "info": {"crawler_name": "c1", "response_code": 3}},
                    {"id": 2, "status": 0, "info": {"crawler_name": "c2", "response_code": 3}},
                    {"id": 3, "status": 0, "info": None},
                    {"id": 4, "status": 0, "info": None}
                ])
                for pk, resource in enumerate(self.resources):
                    if (resource["status"] == self.statusCodes["SUCCEDED"]): self.statusRecords[resource["status"]] += 1
                    else: self.statusRecords[resource["status"]].append(pk)
                    if (self.config["uniqueresourceid"]): 
                        if (resource["id"] not in self.IDsHash): self.IDsHash[resource["id"]] = (self.resources, pk)
                        else: raise KeyError("Duplicated ID found in resources list: %s." % resource["id"])
            
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
            if (changeInfo): list[pk]["info"] = info
        else: 
            list.append({"id": id, "status": status, "info": info})
    
    def select(self): 
        try: pk = self.statusRecords[self.statusCodes["AVAILABLE"]].popleft()
        except IndexError: return (None, None, None)
        self._save(self.resources, pk, None, self.statusCodes["INPROGRESS"], None, False)
        self.statusRecords[self.statusCodes["INPROGRESS"]].append(pk)
        return (pk, self.resources[pk]["id"], deepcopy(self.resources[pk]["info"]))
    
    def update(self, resourceKey, status, resourceInfo): 
        currentStatus = self.resources[resourceKey]["status"]
        self.statusRecords[currentStatus].remove(resourceKey)
        if (resourceInfo): self._save(self.resources, resourceKey, None, status, resourceInfo)
        else: self._save(self.resources, resourceKey, None, status, resourceInfo, False)
        if (status == self.statusCodes["SUCCEDED"]): self.statusRecords[status] += 1
        else: self.statusRecords[status].append(resourceKey)
        
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
                if (not self.config["separateinsertlist"]): self.statusRecords[self.statusCodes["AVAILABLE"]].append(len(insertList))
                if (self.config["uniqueresourceid"]): self.IDsHash[resourceID] = (insertList, len(insertList))
                self._save(insertList, None, resourceID, self.statusCodes["AVAILABLE"], resourceInfo)
        
    def count(self): 
        return (len(self.resources), 
                self.statusRecords[self.statusCodes["SUCCEDED"]], 
                len(self.statusRecords[self.statusCodes["INPROGRESS"]]), 
                len(self.statusRecords[self.statusCodes["AVAILABLE"]]), 
                len(self.statusRecords[self.statusCodes["FAILED"]]), 
                len(self.statusRecords[self.statusCodes["ERROR"]]))
        
    def reset(self, status): 
        resetList = self.statusRecords[status][:]
        for pk in resetList:
            self.statusRecords[status].remove(pk)
            self._save(self.resources, pk, None, self.statusCodes["AVAILABLE"], None, False)
            self.statusRecords[self.statusCodes["AVAILABLE"]].appendleft(pk)
        return len(resetList)
        
        
class FilePersistenceHandler(MemoryPersistenceHandler):
    saveLock = threading.Lock()
    lastSaveTime = None

    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        with self.loadLock:
            if (not self.resources):
                with open(self.config["selectfilename"], "r") as resourcesFile: resourcesList = json.load(resourcesFile)
                for resource in resourcesList:
                    if (resource["status"] == self.statusCodes["SUCCEDED"]): self.statusRecords[resource["status"]] += 1
                    else: self.statusRecords[resource["status"]].append(len(self.resources))
                    if (self.config["uniqueresourceid"]): 
                        if (resource["id"] not in self.IDsHash): 
                            self.IDsHash[resource["id"]] = (self.resources, len(self.resources))
                        else: raise KeyError("Duplicated ID found in resources file: %s." % resource["id"])
                    if ("info" not in resource): resource["info"] = None
                    self.resources.append(resource)
            
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        
        if ("uniqueresourceid" not in self.config): self.config["uniqueresourceid"] = False
        else: self.config["uniqueresourceid"] = common.str2bool(self.config["uniqueresourceid"])
    
        if ("ondupkeyupdate" not in self.config): self.config["ondupkeyupdate"] = False
        else: self.config["ondupkeyupdate"] = common.str2bool(self.config["ondupkeyupdate"])
        
        if ("insertfilename" not in self.config): self.config["insertfilename"] = self.config["selectfilename"]
        
        if (self.config["insertfilename"] == self.config["selectfilename"]): self.config["separateinsertlist"] = False
        else: self.config["separateinsertlist"] = True
        
        if ("savetimedelta" not in self.config) or (int(self.config["savetimedelta"]) < 1): self.config["savetimedelta"] = 60
        else: self.config["savetimedelta"] = int(self.config["savetimedelta"])
        
    def _save(self, list, pk, id, status, info, changeInfo = True):
        with self.saveLock: MemoryPersistenceHandler._save(self, list, pk, id, status, info, changeInfo)
        
    def _dump(self):
        with self.saveLock:
            if (not FilePersistenceHandler.lastSaveTime): FilePersistenceHandler.lastSaveTime = datetime.now()
            elapsedTime = datetime.now() - FilePersistenceHandler.lastSaveTime
            if (elapsedTime.seconds >= self.config["savetimedelta"]):
                with open(self.config["selectfilename"], "w") as output: json.dump(self.resources, output)
                if (self.config["separateinsertlist"]): 
                    with open(self.config["insertfilename"], "w") as output: json.dump(self.insertedResources, output)
                FilePersistenceHandler.lastSaveTime = datetime.now()
        
    def update(self, resourceKey, status, resourceInfo): 
        MemoryPersistenceHandler.update(self, resourceKey, status, resourceInfo)
        self._dump()
        
    def insert(self, resourcesList): 
        MemoryPersistenceHandler.insert(self, resourcesList)
        self._dump()
        
    def reset(self, status):    
        resetedCount = MemoryPersistenceHandler.reset(self, status)    
        self._dump()
        return resetedCount

    def shutdown(self): 
        with self.saveLock:
            with open(self.config["selectfilename"], "w") as output: json.dump(self.resources, output)
            if (self.config["separateinsertlist"]): 
                with open(self.config["insertfilename"], "w") as output: json.dump(self.insertedResources, output)
        
        
class MySQLPersistenceHandler(BasePersistenceHandler):
    def __init__(self, configurationsDictionary):
        self._extractConfig(configurationsDictionary)
        self.mysqlConnection = mysql.connector.connect(user=self.selectConfig["user"], password=self.selectConfig["password"], host=self.selectConfig["host"], database=self.selectConfig["name"])
        self.lastSelectID = None
                
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        self.selectConfig = configurationsDictionary["select"]
        if ("insert" not in configurationsDictionary): self.insertConfig = self.selectConfig
        else: self.insertConfig = configurationsDictionary["insert"]
    
        # Set default values
        if ("ondupkeyupdate" not in self.config): self.config["ondupkeyupdate"] = False
        else: self.config["ondupkeyupdate"] = common.str2bool(self.config["ondupkeyupdate"])
        
        if ("infocolumn" not in self.selectConfig): self.selectConfig["infocolumn"] = []
        elif (not isinstance(self.selectConfig["infocolumn"], list)): self.selectConfig["infocolumn"] = [self.selectConfig["infocolumn"]]
        
        if ("user" not in self.insertConfig): self.insertConfig["user"] = self.selectConfig["user"]
        if ("password" not in self.insertConfig): self.insertConfig["password"] = self.selectConfig["password"]
        if ("host" not in self.insertConfig): self.insertConfig["host"] = self.selectConfig["host"]
        if ("name" not in self.insertConfig): self.insertConfig["name"] = self.selectConfig["name"]
        if ("table" not in self.insertConfig): self.insertConfig["table"] = self.selectConfig["table"]
        if ("infocolumn" not in self.insertConfig): self.insertConfig["infocolumn"] = self.selectConfig["infocolumn"]
        elif (not isinstance(self.insertConfig["infocolumn"], list)): self.insertConfig["infocolumn"] = [self.insertConfig["infocolumn"]]
        
    def select(self):
        cursor = self.mysqlConnection.cursor()
        query = "UPDATE " + self.selectConfig["table"] + " SET resources_pk = LAST_INSERT_ID(resources_pk), status = %s WHERE status = %s ORDER BY resources_pk LIMIT 1"
        cursor.execute(query, (self.statusCodes["INPROGRESS"], self.statusCodes["AVAILABLE"]))
        query = "SELECT " + ", ".join(["resources_pk", "resource_id"] + self.selectConfig["infocolumn"]) + " FROM " + self.selectConfig["table"] + " WHERE resources_pk = LAST_INSERT_ID()"
        cursor.execute(query)
        resource = cursor.fetchone()
        self.mysqlConnection.commit()
        cursor.close()
        if (resource) and (resource[0] != self.lastSelectID): 
            self.lastSelectID = resource[0]
            return (resource[0], resource[1], dict(zip(self.selectConfig["infocolumn"], resource[2:])))
        else: return (None, None, None)
        
    def update(self, resourceKey, status, resourceInfo):
        cursor = self.mysqlConnection.cursor()
        if (not resourceInfo): 
            query = "UPDATE " + self.selectConfig["table"] + " SET status = %s WHERE resources_pk = %s"
            cursor.execute(query, (status, resourceKey))
        else: 
            query = "UPDATE " + self.selectConfig["table"] + " SET status = %s, " + " = %s, ".join(resourceInfo.keys()) + " = %s WHERE resources_pk = %s"
            cursor.execute(query, (status,) + tuple(resourceInfo.values()) + (resourceKey,))
        self.mysqlConnection.commit()
        cursor.close()
        
    def insert(self, resourcesList):
        cursor = self.mysqlConnection.cursor()
        query = "INSERT INTO " + self.insertConfig["table"] + " (" + ", ".join(["resource_id"] + self.insertConfig["infocolumn"]) + ") VALUES "
        
        data = []
        values = []
        for resourceID, resourceInfo in resourcesList: 
            resourceValues = [str(resourceID)]
            if (not resourceInfo): resourceInfo = {}
            for column in self.insertConfig["infocolumn"]:
                if (column in resourceInfo): 
                    resourceValues.append("%s")
                    data.append(resourceInfo[column])
                else: resourceValues.append("DEFAULT")
            values.append("(" + ", ".join(resourceValues) + ")") 
            
        query += ", ".join(values)
        if (self.config["ondupkeyupdate"]):
            query += " ON DUPLICATE KEY UPDATE resource_id = VALUES(resource_id), " + ", ".join(["{0} = VALUES({0})".format(column) for column in self.insertConfig["infocolumn"]])
            column 
        
        cursor.execute(query, data)
        self.mysqlConnection.commit()        
        cursor.close()
        
    def count(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT status, count(*) FROM " + self.selectConfig["table"] + " GROUP BY status"
        
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        
        counts = [0, 0, 0, 0, 0, 0]
        for row in result:
            if (row[0] == self.statusCodes["SUCCEDED"]): counts[1] = row[1]
            elif (row[0] == self.statusCodes["INPROGRESS"]): counts[2] = row[1]
            elif (row[0] == self.statusCodes["AVAILABLE"]): counts[3] = row[1]
            elif (row[0] == self.statusCodes["FAILED"]): counts[4] = row[1]
            elif (row[0] == self.statusCodes["ERROR"]): counts[5] = row[1]
            counts[0] += row[1]
        
        return tuple(counts)
        
    def reset(self, status):
        cursor = self.mysqlConnection.cursor()
        query = "UPDATE " + self.selectConfig["table"] + " SET status = %s WHERE status = %s"
        cursor.execute(query, (self.statusCodes["AVAILABLE"], status))
        affectedRows = cursor.rowcount
        self.mysqlConnection.commit()
        cursor.close()
        return affectedRows
        
    def close(self):
        self.mysqlConnection.close()
        