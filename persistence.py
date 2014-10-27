# -*- coding: iso-8859-1 -*-

import logging
import threading
import Queue
import mysql.connector

import time
class BasePersistenceHandler():  
    statusCodes = {"AVAILABLE":  0, 
                   "COLLECTING": 1, 
                   "INSERTING":  3, 
                   "SUCCEDED":   4, 
                   "FAILED":    -1}

    def __init__(self, configurationDictionary): pass # all configurations in the XML are passed to the persistence class
    def selectResource(self): return (None, None) # (resource id, resource info dictionary)
    def updateResource(self, resourceID, resourceInfo, status, crawler): pass
    def insertResource(self, fromResourceID, newResourceID, newResourceInfo, crawler): pass
    def totalResourcesCount(self): return 0
    def resourcesCollectedCount(self): return 0
    def resourcesSucceededCount(self): return 0
    def resourcesFailedCount(self): return 0
    def close(self): pass
        

class MySQLPersistenceHandler(BasePersistenceHandler):
    insertThread = None
    startInsertThreadLock = threading.Lock()
    insertQueue = Queue.Queue()

    def __init__(self, configurationDictionary):
        self._extractConfig(configurationDictionary)
        self.mysqlConnection = mysql.connector.connect(user=self.selectConfig["user"], password=self.selectConfig["password"], host=self.selectConfig["host"], database=self.selectConfig["name"])
        with MySQLPersistenceHandler.startInsertThreadLock:
            if ((not MySQLPersistenceHandler.insertThread) and configurationDictionary["global"]["feedback"]):
                MySQLPersistenceHandler.insertThread = threading.Thread(target = self._doInsert)
                MySQLPersistenceHandler.insertThread.daemon = True
                MySQLPersistenceHandler.insertThread.start()
                
    def _extractConfig(self, configurationDictionary):
        self.feedbackConfig = configurationDictionary["global"]["feedback"]
        self.selectConfig = configurationDictionary["persistence"]["mysql"]["select"]
    
        # Set default values
        if ("resourceinfo" not in self.selectConfig): self.selectConfig["resourceinfo"] = []
        elif (not isinstance(self.selectConfig["resourceinfo"], list)): self.selectConfig["resourceinfo"] = [self.selectConfig["resourceinfo"]]
        
        if ("insert" not in configurationDictionary["persistence"]["mysql"]): self.insertConfig = self.selectConfig
        else: self.insertConfig = configurationDictionary["persistence"]["mysql"]["insert"]
        
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
        
    def insertResource(self, fromResourceID, newResourceID, newResourceInfo, crawler):
        # Interrupt execution if insertion thread is not alive
        if (not MySQLPersistenceHandler.insertThread.is_alive()): 
            logging.error("Insertion thread is not alive.")
            persist.updateResource(fromResourceID, None, status["FAILED"], crawler)
            raise RuntimeError("Insertion thread is not alive.")
        MySQLPersistenceHandler.insertQueue.put((fromResourceID, newResourceID, newResourceInfo, crawler))
    
    def _doInsert(self):    
        insertConnection = mysql.connector.connect(user=self.insertConfig["user"], password=self.insertConfig["password"], host=self.insertConfig["host"], database=self.insertConfig["name"])
        cursor = insertConnection.cursor()
        while (True):
            (fromResourceID, newResourceID, newResourceInfo, crawler) = MySQLPersistenceHandler.insertQueue.get()
            try: 
                time.sleep(5)
                if not newResourceInfo:
                    query = "INSERT INTO " + self.insertConfig["table"] + " (resource_id, crawler) VALUES (%s, %s)"
                    if (self.feedbackConfig["overwrite"]): query += " ON DUPLICATE KEY UPDATE crawler = VALUES(crawler)"
                    cursor.execute(query, (newResourceID, crawler))
                    insertConnection.commit()
                else:
                    query = "INSERT INTO " + self.insertConfig["table"] + " (" + ", ".join(["resource_id", "crawler"] + newResourceInfo.keys()) + ") VALUES (" + ", ".join(["%s", "%s"] + ["%s"] * len(newResourceInfo)) + ")"
                    if (self.feedbackConfig["overwrite"]):
                        query += " ON DUPLICATE KEY UPDATE crawler = VALUES(crawler), " + ", ".join(["{0} = VALUES({0})".format(key) for key in newResourceInfo.keys()])
                    cursor.execute(query, (newResourceID, crawler) + tuple(newResourceInfo.values()))
                    insertConnection.commit()
            except:
                # In case of error, mark the resource from wich the new resources came from as failed
                logging.exception("Exception while inserting the new resource '%s' sent by crawler '%s' as a result of crawling the resource '%s'." % (newResourceID, crawler,fromResourceID))
                self.updateResource(fromResourceID, None, self.statusCodes["FAILED"], crawler)
            else:
                # If everything is right, mark the resource as succeded
                self.updateResource(fromResourceID, None, self.statusCodes["SUCCEDED"], crawler)
        
    def totalResourcesCount(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"]
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def resourcesCollectedCount(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status = %s OR status = %s"
        cursor.execute(query, (self.statusCodes["SUCCEDED"], self.statusCodes["FAILED"]))
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
        