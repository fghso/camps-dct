# -*- coding: iso-8859-1 -*-

import logging
import mysql.connector
from mysql.connector.errors import Error
from mysql.connector import errorcode


class BasePersistenceHandler():  
    statusCodes = {"SUCCEDED":   2,
                   "INPROGRESS": 1,
                   "AVAILABLE":  0, 
                   "FAILED":    -1,
                   "ERROR":     -2}

    def __init__(self, configurationsDictionary): pass # All configurations in the XML are passed to the persistence class
    def select(self): return (None, None) # Return a tuple: (resource id, resource info dictionary)
    def update(self, resourceID, status, resourceInfo): pass
    def insert(self, resourceID, resourceInfo): return True # Return True if success or False otherwise
    def count(self): return (0, 0, 0, 0, 0, 0) # Return a tuple: (total, succeeded, inprogress, available, failed, error)
    def reset(self, status): return 0 # Return the number of resources reseted
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
        
    def select(self):
        cursor = self.mysqlConnection.cursor()
        query = "SELECT " + ", ".join(["resource_id"] + self.selectConfig["resourceinfo"]) + " FROM " + self.selectConfig["table"] + " WHERE status = %s ORDER BY resources_pk LIMIT 1"
        cursor.execute(query, (self.statusCodes["AVAILABLE"],))
        resource = cursor.fetchone()
        self.mysqlConnection.commit()
        cursor.close()
        if resource: return (resource[0], dict(zip(self.selectConfig["resourceinfo"], resource[1:])))
        else: return (None, None)
        
    def update(self, resourceID, status, resourceInfo):
        cursor = self.mysqlConnection.cursor()
        if not resourceInfo: 
            query = "UPDATE " + self.selectConfig["table"] + " SET status = %s WHERE resource_id = %s"
            cursor.execute(query, (status, resourceID))
        else: 
            query = "UPDATE " + self.selectConfig["table"] + " SET status = %s, " + " = %s, ".join(resourceInfo.keys()) + " = %s WHERE resource_id = %s"
            cursor.execute(query, (status,) + tuple(resourceInfo.values()) + (resourceID,))
        self.mysqlConnection.commit()
        cursor.close()
        
    def insert(self, resourceID, resourceInfo):
        cursor = self.mysqlConnection.cursor()
        try: 
            if not resourceInfo:
                query = "INSERT INTO " + self.insertConfig["table"] + " (resource_id) VALUES (%s)"
                cursor.execute(query, (resourceID,))
                self.mysqlConnection.commit()
            else:
                query = "INSERT INTO " + self.insertConfig["table"] + " (" + ", ".join(["resource_id"] + resourceInfo.keys()) + ") VALUES (" + ", ".join(["%s"] + (["%s"] * len(resourceInfo))) + ")"
                cursor.execute(query, (resourceID,) + tuple(resourceInfo.values()))
                self.mysqlConnection.commit()
        except mysql.connector.Error as err:
            if err.errno != errorcode.ER_DUP_ENTRY:
                logging.exception("Exception inserting resource.")
                return False
            else: return True
        else: return True
        finally: cursor.close()
        
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
        