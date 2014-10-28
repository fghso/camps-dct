# -*- coding: iso-8859-1 -*-

import logging
import threading
import mysql.connector
from mysql.connector.errors import Error
from mysql.connector import errorcode


class BasePersistenceHandler():  
    statusCodes = {"AVAILABLE":  0, 
                   "INPROGRESS": 1, 
                   "SUCCEDED":   2, 
                   "FAILED":    -1}

    def __init__(self, configurationsDictionary): pass # all configurations in the XML are passed to the persistence class
    def selectResource(self): return (None, None) # return (resource id, resource info dictionary)
    def updateResource(self, resourceID, resourceInfo, status, crawler): pass
    def insertResource(self, resourceID, resourceInfo, crawler): return True # return True or False
    def totalResourcesCount(self): return 0
    def resourcesAvailableCount(self): return 0
    def resourcesInProgressCount(self): return 0
    def resourcesSucceededCount(self): return 0
    def resourcesFailedCount(self): return 0
    def close(self): pass
        

class MySQLPersistenceHandler(BasePersistenceHandler):
    def __init__(self, configurationsDictionary):
        self._extractConfig(configurationsDictionary)
        self.mysqlConnection = mysql.connector.connect(user=self.selectConfig["user"], password=self.selectConfig["password"], host=self.selectConfig["host"], database=self.selectConfig["name"])
                
    def _extractConfig(self, configurationsDictionary):
        self.selectConfig = configurationsDictionary["persistence"]["handler"]["select"]
    
        # Set default values
        if ("resourceinfo" not in self.selectConfig): self.selectConfig["resourceinfo"] = []
        elif (not isinstance(self.selectConfig["resourceinfo"], list)): self.selectConfig["resourceinfo"] = [self.selectConfig["resourceinfo"]]
        
        if ("insert" not in configurationsDictionary["persistence"]["handler"]): self.insertConfig = self.selectConfig
        else: self.insertConfig = configurationsDictionary["persistence"]["handler"]["insert"]
        
        if ("user" not in self.insertConfig): self.insertConfig["user"] = self.selectConfig["user"]
        if ("password" not in self.insertConfig): self.insertConfig["password"] = self.selectConfig["password"]
        if ("host" not in self.insertConfig): self.insertConfig["host"] = self.selectConfig["host"]
        if ("name" not in self.insertConfig): self.insertConfig["name"] = self.selectConfig["name"]
        if ("table" not in self.insertConfig): self.insertConfig["table"] = self.selectConfig["table"]
        
    def selectResource(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT " + ", ".join(["resource_id"] + self.selectConfig["resourceinfo"]) + " FROM " + self.selectConfig["table"] + " WHERE status = %s ORDER BY resources_pk LIMIT 1"
        cursor.execute(query, (self.statusCodes["AVAILABLE"],))
        resource = cursor.fetchone()
        self.mysqlConnection.commit()
        cursor.close()
        if resource: return (resource[0], dict(zip(self.selectConfig["resourceinfo"], resource[1:])))
        else: return (None, None)
        
    def updateResource(self, resourceID, resourceInfo, status, crawler):
        cursor = self.mysqlConnection.cursor()
        if not resourceInfo: 
            query = "UPDATE " + self.selectConfig["table"] + " SET status = %s, crawler = %s WHERE resource_id = %s"
            cursor.execute(query, (status, crawler, resourceID))
        else: 
            query = "UPDATE " + self.selectConfig["table"] + " SET status = %s, crawler = %s, " + " = %s, ".join(resourceInfo.keys()) + " = %s WHERE resource_id = %s"
            cursor.execute(query, (status, crawler) + tuple(resourceInfo.values()) + (resourceID,))
        self.mysqlConnection.commit()
        cursor.close()
        
    def insertResource(self, resourceID, resourceInfo, crawler):
        cursor = self.mysqlConnection.cursor()
        try: 
            if not resourceInfo:
                query = "INSERT INTO " + self.insertConfig["table"] + " (resource_id, crawler) VALUES (%s, %s)"
                cursor.execute(query, (resourceID, crawler))
                self.mysqlConnection.commit()
            else:
                query = "INSERT INTO " + self.insertConfig["table"] + " (" + ", ".join(["resource_id", "crawler"] + resourceInfo.keys()) + ") VALUES (" + ", ".join(["%s", "%s"] + ["%s"] * len(resourceInfo)) + ")"
                cursor.execute(query, (resourceID, crawler) + tuple(resourceInfo.values()))
                self.mysqlConnection.commit()
        except mysql.connector.Error as err:
            if err.errno != errorcode.ER_DUP_ENTRY:
                logging.exception("Exception while inserting the new resource %s sent by crawler '%s'." % (resourceID, crawler))
                return False
            else: return True
        else: return True
        
    def totalResourcesCount(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"]
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def resourcesAvailableCount(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status = %s"
        cursor.execute(query, (self.statusCodes["AVAILABLE"],))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def resourcesInProgressCount(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status = %s"
        cursor.execute(query, (self.statusCodes["INPROGRESS"],))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def resourcesSucceededCount(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status = %s"
        cursor.execute(query, (self.statusCodes["SUCCEDED"],))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    
    def resourcesFailedCount(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status = %s"
        cursor.execute(query, (self.statusCodes["FAILED"],))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def close(self):
        self.mysqlConnection.close()
        