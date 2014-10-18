# -*- coding: iso-8859-1 -*-

import mysql.connector


class BasePersistenceHandler():  
    statusCodes = {"AVAILABLE":  0, 
                   "INPROGRESS": 1, 
                   "SUCCEDED":   2, 
                   "FAILED":    -2}

    def selectResource(self):
        return("resourceID", "responseCode", "annotation")
        
    def updateResource(self, resourceID, status, amount, crawler):
        pass
        
    def totalResourcesCount(self):
        return 0
    
    def resourcesCollectedCount(self):
        return 0
    
    def resourcesSucceededCount(self):
        return 0
    
    def resourcesFailedCount(self):
        return 0
        

class MySQLPersistenceHandler(BasePersistenceHandler):
    def __init__(self, dbUser, dbPassword, dbHost, dbName, dbTable):
        self.database = dbName
        self.table = dbTable
        self.mysqlConnection = mysql.connector.connect(user=dbUser, password=dbPassword, host=dbHost, database=dbName)
        
    def selectResource(self):
        query = "SELECT resource_id, response_code, annotation FROM " + self.table + " WHERE status = %s ORDER BY resources_pk LIMIT 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (statusCodes["AVAILABLE"],))
        resource = cursor.fetchone()
        return resource if (resource) else [None] * 3
        
    def updateResource(self, resourceID, status, responseCode, annotation, crawler):
        query = "UPDATE " + self.table + " SET status = %s, response_code = %s, annotation = %s, crawler = %s WHERE resource_id = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (status, responseCode, annotation, crawler, resourceID))
        self.mysqlConnection.commit()
        
    def totalResourcesCount(self):
        query = "SELECT count(resource_id) FROM " + self.table
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]
        
    def resourcesCollectedCount(self):
        query = "SELECT count(resource_id) FROM " + self.table + " WHERE status != %s AND status != %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (statusCodes["AVAILABLE"], statusCodes["INPROGRESS"]))
        return cursor.fetchone()[0]
        
    def resourcesSucceededCount(self):
        query = "SELECT count(resource_id) FROM " + self.table + " WHERE status = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (statusCodes["SUCCEDED"],))
        return cursor.fetchone()[0]
    
    def resourcesFailedCount(self):
        query = "SELECT count(resource_id) FROM " + self.table + " WHERE status = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (statusCodes["FAILED"],))
        return cursor.fetchone()[0]
        