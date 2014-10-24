# -*- coding: iso-8859-1 -*-

import logging
import threading
import Queue
import mysql.connector


class BasePersistenceHandler():  
    statusCodes = {"AVAILABLE":  0, 
                   "INPROGRESS": 1, 
                   "SUCCEDED":   2, 
                   "FAILED":    -2}

    def __init__(self, configurationDictionary): pass # all configurations in the XML are passed to the persistence class
    def selectResource(self): return (None, None) # (resource id, resource info dictionary)
    def updateResource(self, resourceID, resourceInfo, status, crawler): pass
    def insertResource(self, resourceID, resourceInfo, crawler): pass
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
        self.selectConfig = configurationDictionary["persistence"]["mysql"]["select"]
        self.insertConfig = configurationDictionary["persistence"]["mysql"]["insert"]
        self.mysqlConnection = mysql.connector.connect(user=self.selectConfig["user"], password=self.selectConfig["password"], host=self.selectConfig["host"], database=self.selectConfig["name"])
        with MySQLPersistenceHandler.startInsertThreadLock:
            if ((not MySQLPersistenceHandler.insertThread) and configurationDictionary["global"]["feedback"]):
                MySQLPersistenceHandler.insertThread = threading.Thread(target = self._doInsert)
                MySQLPersistenceHandler.insertThread.daemon = True
                MySQLPersistenceHandler.insertThread.start()
        
    def selectResource(self):
        query = "SELECT resource_id, response_code, annotation FROM " + self.selectConfig["table"] + " WHERE status = %s ORDER BY resources_pk LIMIT 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (self.statusCodes["AVAILABLE"],))
        resource = cursor.fetchone()
        self.mysqlConnection.commit()
        cursor.close()
        if resource: return (resource[0], {"responsecode": resource[1], "annotation": resource[2]})
        else: return (None, None)
        
    def updateResource(self, resourceID, resourceInfo, status, crawler):
        query = "UPDATE " + self.selectConfig["table"] + " SET status = %s, response_code = %s, annotation = %s, crawler = %s WHERE resource_id = %s"
        cursor = self.mysqlConnection.cursor()
        if not resourceInfo: cursor.execute(query, (status, None, None, crawler, resourceID))
        else: cursor.execute(query, (status, resourceInfo["responsecode"], resourceInfo["annotation"], crawler, resourceID))
        self.mysqlConnection.commit()
        cursor.close()
        
    def insertResource(self, resourceID, resourceInfo, crawler): 
        MySQLPersistenceHandler.insertQueue.put((resourceID, resourceInfo, crawler))
    
    def _doInsert(self):    
        insertConnection = mysql.connector.connect(user=self.selectConfig["user"], password=self.selectConfig["password"], host=self.selectConfig["host"], database=self.selectConfig["name"])
        cursor = insertConnection.cursor()
        query = "INSERT INTO " + self.selectConfig["table"] + " (resource_id, response_code, annotation, crawler) VALUES (%s, %s, %s, %s)"
        if (self.insertConfig["overwrite"]):
            query += " ON DUPLICATE KEY UPDATE response_code = VALUES(response_code), annotation = VALUES(annotation), crawler = VALUES(crawler)"
        while (True):
            (resourceID, resourceInfo, crawler) = MySQLPersistenceHandler.insertQueue.get()
            try: 
                cursor.execute(query, (resourceID, resourceInfo["responsecode"], resourceInfo["annotation"], crawler))
                insertConnection.commit()
            except: 
                logging.exception("Exception while inserting the new resource '%s' sent by crawler '%s'." % (resourceID, crawler))
        
    def totalResourcesCount(self):
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"]
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def resourcesCollectedCount(self):
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status != %s AND status != %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (self.statusCodes["AVAILABLE"], self.statusCodes["INPROGRESS"]))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def resourcesSucceededCount(self):
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (self.statusCodes["SUCCEDED"],))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    
    def resourcesFailedCount(self):
        query = "SELECT count(resource_id) FROM " + self.selectConfig["table"] + " WHERE status = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (self.statusCodes["FAILED"],))
        count = cursor.fetchone()[0]
        cursor.close()
        return count
        
    def close(self):
        self.mysqlConnection.close()
        