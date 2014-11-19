# -*- coding: iso-8859-1 -*-

import threading
import json
import logging
import common
from collections import deque
from copy import deepcopy
import mysql.connector
from mysql.connector.errors import Error
from mysql.connector import errorcode


class BasePersistenceHandler():  
    statusCodes = {"SUCCEDED":   2,
                   "INPROGRESS": 1,
                   "AVAILABLE":  0, 
                   "FAILED":    -1,
                   "ERROR":     -2}

    def __init__(self, configurationsDictionary): pass # Receives everything in handler section of the XML configurations file
    def select(self): return (None, None, None) # Return a tuple: (resource unique key, resource id, resource info dictionary)
    def update(self, resourceKey, status, resourceInfo): pass
    def insert(self, resourceID, resourceInfo): pass
    def count(self): return (0, 0, 0, 0, 0, 0) # Return a tuple: (total, succeeded, inprogress, available, failed, error)
    def reset(self, status): return 0 # Return the number of resources reseted
    def close(self): pass
        
        
# This class was built as basis for FilePersistenceHandler and for test purposes. 
# It is not intended for direct use in a production enviroment
class MemoryPersistenceHandler(BasePersistenceHandler):
    loadResourcesLock = threading.Lock()
    resources = []
    IDsHash = {}
    statusRecords = {BasePersistenceHandler.statusCodes["SUCCEDED"]:   0,
                     BasePersistenceHandler.statusCodes["INPROGRESS"]: deque(),
                     BasePersistenceHandler.statusCodes["AVAILABLE"]:  deque(), 
                     BasePersistenceHandler.statusCodes["FAILED"]:     deque(),
                     BasePersistenceHandler.statusCodes["ERROR"]:      deque()}

    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        with self.loadResourcesLock:
            if (not self.resources):
                # Some data for tests
                self.resources.extend([
                    {"id": 1, "status": 0, "info": {"crawler_name": "c1", "response_code": 3}},
                    {"id": 2, "status": 0, "info": {"crawler_name": "c2", "response_code": 3}},
                    {"id": 3, "status": 0, "info": None},
                    {"id": 4, "status": 0, "info": None}
                ])
                for pk, resource in enumerate(self.resources):
                    if (resource["status"] == self.statusCodes["SUCCEDED"]): self.statusRecords[self.statusCodes["SUCCEDED"]] += 1
                    else: self.statusRecords[resource["status"]].append(pk)
                    if (self.config["uniqueresourceid"]): 
                        if not (resource["id"] in self.IDsHash): self.IDsHash[resource["id"]] = pk
                        else: raise KeyError("Duplicated ID found in resources list: %s." % resource["id"])
            
    def _extractConfig(self, configurationsDictionary):
        # Global
        self.config = configurationsDictionary
        
        if ("uniqueresourceid" not in self.config): self.config["uniqueresourceid"] = False
        else: self.config["uniqueresourceid"] = common.str2bool(self.config["uniqueresourceid"])
    
        # Insert
        if ("insert" not in configurationsDictionary): self.insertConfig = {}
        else: self.insertConfig = configurationsDictionary["insert"]
    
        if ("ondupkeyupdate" not in self.insertConfig): self.insertConfig["ondupkeyupdate"] = False
        else: self.insertConfig["ondupkeyupdate"] = common.str2bool(self.insertConfig["ondupkeyupdate"])
    
    def select(self): 
        if (self.statusRecords[self.statusCodes["AVAILABLE"]]): 
            pk = self.statusRecords[self.statusCodes["AVAILABLE"]][0]
            return (pk, self.resources[pk]["id"], deepcopy(self.resources[pk]["info"]))
        else: return (None, None, None)
    
    def update(self, resourceKey, status, resourceInfo): 
        resource = self.resources[resourceKey]
        self.statusRecords[resource["status"]].remove(resourceKey)
        resource["status"] = status
        if (resourceInfo): resource["info"] = resourceInfo
        if (status == self.statusCodes["SUCCEDED"]): self.statusRecords[self.statusCodes["SUCCEDED"]] += 1
        else: self.statusRecords[status].append(resourceKey)
        
    def insert(self, resourceID, resourceInfo): 
        if (self.config["uniqueresourceid"]):
            if (resourceID in self.IDsHash):
                if (self.insertConfig["ondupkeyupdate"]): 
                    self.resources[self.IDsHash[resourceID]]["info"] = resourceInfo
                    return
                else: raise KeyError("Cannot insert resource, ID %s already exists." % resourceID)
        self.resources.append({"id": resourceID, "status": self.statusCodes["AVAILABLE"], "info": resourceInfo})
        self.statusRecords[self.statusCodes["AVAILABLE"]].append(len(self.resources) - 1)
        if (self.config["uniqueresourceid"]): self.IDsHash[resourceID] = len(self.resources) - 1
        for rsc in self.resources: print rsc
        print self.statusRecords[0]
        
    def count(self): 
        return (len(self.resources), 
                self.statusRecords[self.statusCodes["SUCCEDED"]], 
                len(self.statusRecords[self.statusCodes["INPROGRESS"]]), 
                len(self.statusRecords[self.statusCodes["AVAILABLE"]]), 
                len(self.statusRecords[self.statusCodes["FAILED"]]), 
                len(self.statusRecords[self.statusCodes["ERROR"]]))
        
    def reset(self, status): 
        for pk in self.statusRecords[status]:
            self.resources[pk]["status"] = self.statusCodes["AVAILABLE"]
            self.statusRecords[self.statusCodes["AVAILABLE"]].append(pk)
            
        resetedCount = len(self.statusRecords[status])
        self.statusRecords[status].clear()
            
        return resetedCount
        
        
class FilePersistenceHandler(MemoryPersistenceHandler):
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        with self.loadResourcesLock:
            if (not self.resources):
                with open(self.selectConfig["filename"], "r") as resourcesFile: resourcesList = json.load(resourcesFile)
                for resource in resourcesList:
                    self.resources.append(resource)
                    if ("info" not in resource): resource["info"] = None
                    if (resource["status"] == self.statusCodes["SUCCEDED"]): self.statusRecords[self.statusCodes["SUCCEDED"]] += 1
                    else: self.statusRecords[resource["status"]].append(len(self.resources) - 1)
                    if (self.config["uniqueresourceid"]): 
                        if not (resource["id"] in self.IDsHash): self.IDsHash[resource["id"]] = len(self.resources) - 1
                        else: raise KeyError("Duplicated ID found in resources file: %s." % resource["id"])
            
    def _extractConfig(self, configurationsDictionary):
        # Global
        self.config = configurationsDictionary
        
        if ("uniqueresourceid" not in self.config): self.config["uniqueresourceid"] = False
        else: self.config["uniqueresourceid"] = common.str2bool(self.config["uniqueresourceid"])
    
        # Select
        self.selectConfig = configurationsDictionary["select"]
    
        # Insert
        if ("insert" not in configurationsDictionary): self.insertConfig = {}
        else: self.insertConfig = configurationsDictionary["insert"]
        
        if ("filename" not in self.insertConfig): self.insertConfig["filename"] = self.selectConfig["filename"]
    
        if ("ondupkeyupdate" not in self.insertConfig): self.insertConfig["ondupkeyupdate"] = False
        else: self.insertConfig["ondupkeyupdate"] = common.str2bool(self.insertConfig["ondupkeyupdate"])
    
    
class MySQLPersistenceHandler(BasePersistenceHandler):
    def __init__(self, configurationsDictionary):
        self._extractConfig(configurationsDictionary)
        self.mysqlConnection = mysql.connector.connect(user=self.selectConfig["user"], password=self.selectConfig["password"], host=self.selectConfig["host"], database=self.selectConfig["name"])
                
    def _extractConfig(self, configurationsDictionary):
        self.selectConfig = configurationsDictionary["select"]
    
        # Set default values
        if ("infocolumn" not in self.selectConfig): self.selectConfig["infocolumn"] = []
        elif (not isinstance(self.selectConfig["infocolumn"], list)): self.selectConfig["infocolumn"] = [self.selectConfig["infocolumn"]]
        
        if ("insert" not in configurationsDictionary): self.insertConfig = self.selectConfig
        else: self.insertConfig = configurationsDictionary["insert"]
        
        if ("user" not in self.insertConfig): self.insertConfig["user"] = self.selectConfig["user"]
        if ("password" not in self.insertConfig): self.insertConfig["password"] = self.selectConfig["password"]
        if ("host" not in self.insertConfig): self.insertConfig["host"] = self.selectConfig["host"]
        if ("name" not in self.insertConfig): self.insertConfig["name"] = self.selectConfig["name"]
        if ("table" not in self.insertConfig): self.insertConfig["table"] = self.selectConfig["table"]
        if ("ondupkeyupdate" not in self.insertConfig): self.insertConfig["ondupkeyupdate"] = False
        else: self.insertConfig["ondupkeyupdate"] = common.str2bool(self.insertConfig["ondupkeyupdate"])
        
    def select(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT " + ", ".join(["resources_pk", "resource_id"] + self.selectConfig["infocolumn"]) + " FROM " + self.selectConfig["table"] + " WHERE status = %s ORDER BY resources_pk LIMIT 1"
        cursor.execute(query, (self.statusCodes["AVAILABLE"],))
        resource = cursor.fetchone()
        self.mysqlConnection.commit()
        cursor.close()
        if (resource): return (resource[0], resource[1], dict(zip(self.selectConfig["infocolumn"], resource[2:])))
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
        
    def insert(self, resourceID, resourceInfo):
        cursor = self.mysqlConnection.cursor()
        if (not resourceInfo):
            query = "INSERT INTO " + self.insertConfig["table"] + " (resource_id) VALUES (%s)"
            if (self.insertConfig["ondupkeyupdate"]): query += " ON DUPLICATE KEY UPDATE resource_id = VALUES(resource_id)"
            cursor.execute(query, (resourceID,))
            self.mysqlConnection.commit()
        else:
            query = "INSERT INTO " + self.insertConfig["table"] + " (" + ", ".join(["resource_id"] + resourceInfo.keys()) + ") VALUES (" + ", ".join(["%s"] + (["%s"] * len(resourceInfo))) + ")"
            if (self.insertConfig["ondupkeyupdate"]):
                query += " ON DUPLICATE KEY UPDATE resource_id = VALUES(resource_id), " + ", ".join(["{0} = VALUES({0})".format(key) for key in resourceInfo.keys()])
            cursor.execute(query, (resourceID,) + tuple(resourceInfo.values()))
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
        