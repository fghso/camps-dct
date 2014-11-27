# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import SocketServer
import threading
import json
import logging
import time
import calendar
import common
import persistence
import filters
from datetime import datetime
from copy import deepcopy


# ==================== Global variables ====================
# Dictionary to store information lists about each client:
#   [network address, process identification (PID), primary key of the resource being collected, 
#    ID of the resource being collected, number of resources already collected, 
#    collection start time and last GET_ID requested time] 
clientsInfo = {} 

# Store a reference for the thread running the client and an event to interrupt its execution
clientsThreads = {}

# Define the next ID to give to a new client
nextFreeID = 1

# Define synchronization objects for critical regions of the code
getIDLock = threading.Lock()
removeClientLock = threading.Lock()
clientRemovedCondition = threading.Condition()
shutdownLock = threading.Lock()


# ==================== Classes ====================
class ServerHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        # Get persistence handler instance
        persistenceHandlerOptions = deepcopy(self.server.config["persistence"]["handler"])
        self.persist = self.server.PersistenceHandlerClass(persistenceHandlerOptions)
        
        # Get filters instances
        self.parallelFilters = []
        self.sequentialFilters = []
        for i, FilterClass in enumerate(self.server.FiltersClasses):
            filterOptions = deepcopy(self.server.config["server"]["filter"][i])
            if (filterOptions["parallel"]): self.parallelFilters.append(FilterClass(filterOptions))
            else: self.sequentialFilters.append(FilterClass(filterOptions))

    def handle(self):
        # Declare global variable
        global nextFreeID
    
        # Define some local variables
        config = self.server.config
        persist = self.persist
        status = self.persist.statusCodes
        client = common.NetworkHandler(self.request)
    
        # Start to handle
        clientID = 0
        running = True
        while (running):
            try: 
                message = client.recv()
                
                # Stop thread execution if the connection has been interrupted
                if (not message): 
                    clientResourceKey = clientsInfo[clientID][2]
                    logging.error("Connection to client %d has been abruptly closed." % clientID)
                    if (config["server"]["verbose"]): print "ERROR: Connection to client %d has been abruptly closed." % clientID
                    persist.update(clientResourceKey, status["ERROR"], None)
                    running = False
                    continue

                command = message["command"]
                
                if (command == "GET_LOGIN"):
                    clientAddress = client.getaddress()
                    clientPid = message["processid"]
                    with shutdownLock:
                        if (self.server.state == "running"):
                            clientID = nextFreeID
                            nextFreeID += 1
                            clientsInfo[clientID] = [clientAddress, clientPid, None, None, -1, datetime.now(), None]
                            clientsThreads[clientID] = (threading.current_thread(), threading.Event())
                        else:
                            client.send({"command": "GIVE_LOGIN", "fail": True, "reason": "Cannot connect, server is %s." % self.server.state})
                            running = False
                            continue
                    client.send({"command": "GIVE_LOGIN", "fail": False, "clientid": clientID})
                    logging.info("New client connected: %d" % clientID)
                    if (config["server"]["verbose"]): print "New client connected: %d" % clientID
                
                elif (command == "GET_ID"):
                    clientStopEvent = clientsThreads[clientID][1]
                    clientsInfo[clientID][2] = None
                    clientsInfo[clientID][3] = None
                    clientsInfo[clientID][4] += 1
                    clientsInfo[clientID][6] = datetime.now()
                    tryagain = True
                    while (tryagain):
                        tryagain = False
                        # If the client hasn't been removed, check resource availability
                        if (not clientStopEvent.is_set()):
                            with getIDLock:
                                (resourceKey, resourceID, resourceInfo) = persist.select()
                                if (resourceID): persist.update(resourceKey, status["INPROGRESS"], None)
                            # If there is a resource available, send ID to client
                            if (resourceID):
                                clientsInfo[clientID][2] = resourceKey
                                clientsInfo[clientID][3] = resourceID
                                clientsInfo[clientID][6] = datetime.now()
                                filters = self.applyFilters(resourceID, resourceInfo)
                                client.send({"command": "GIVE_ID", "resourceid": resourceID, "filters": filters})
                            else:
                                # If there isn't resources available and loopforever is true, wait some time and check again
                                if (config["server"]["loopforever"]): 
                                    time.sleep(5)
                                    tryagain = True
                                # If there isn't resources available and loopforever is false, finish all clients
                                else:
                                    with shutdownLock: 
                                        if (self.server.state == "running"): 
                                            logging.info("Task done, finishing clients...")
                                            if (config["server"]["verbose"]): print "Task done, finishing clients..."
                                            self.server.state = "finishing"
                                            for ID in clientsInfo.keys(): self.removeClient(ID)
                                    tryagain = True
                        # If the client has been removed, finish it
                        else:
                            del clientsInfo[clientID]
                            with clientRemovedCondition: clientRemovedCondition.notify_all()
                            if (self.server.state == "running"):
                                client.send({"command": "FINISH", "reason": "removed"})
                                logging.info("Client %d removed." % clientID)
                                if (config["server"]["verbose"]): print "Client %d removed." % clientID
                            else:
                                if (self.server.state == "finishing"): client.send({"command": "FINISH", "reason": "task done"})
                                if (self.server.state == "shuting down"): client.send({"command": "FINISH", "reason": "shut down"})
                                logging.info("Client %d finished." % clientID)
                                if (config["server"]["verbose"]): print "Client %d finished." % clientID 
                            running = False
                    
                elif (command == "DONE_ID"):
                    clientResourceKey = clientsInfo[clientID][2]
                    clientResourceID = clientsInfo[clientID][3]
                    clientResourceInfo = message["resourceinfo"]
                    if (config["global"]["feedback"]): persist.insert(message["newresources"])
                    persist.update(clientResourceKey, status["SUCCEDED"], clientResourceInfo)
                    client.send({"command": "DONE_RET"})
                            
                elif (command == "EXCEPTION"):
                    clientResourceKey = clientsInfo[clientID][2]
                    clientResourceID = clientsInfo[clientID][3]
                    if (message["type"] == "fail"):
                        logging.warning("Client %s reported fail for resource %s." % (clientID, clientResourceID))
                        persist.update(clientResourceKey, status["FAILED"], None)
                        client.send({"command": "EXCEPTION_RET"})
                    elif (message["type"] == "error"):
                        logging.error("Client %s reported critical error for resource %s. Connection closed." % (clientID, clientResourceID))
                        if (config["server"]["verbose"]): print "ERROR: Critical error on client %s, connection closed." % clientID
                        persist.update(clientResourceKey, status["ERROR"], None)
                        running = False
                    
                elif (command == "GET_STATUS"):
                    # Clients status
                    clientsStatusList = []
                    for (ID, info) in clientsInfo.items():
                        clientThreadState = ((-1 if clientsThreads[ID][1].is_set() else 0) if clientsThreads[ID][0].is_alive() else -2)
                        clientStatus =  {"clientid": ID}
                        clientStatus["threadstate"] = clientThreadState
                        clientStatus["address"] = info[0]
                        clientStatus["pid"] = info[1]
                        clientStatus["resourceid"] = info[3]
                        clientStatus["amount"] = info[4]
                        clientStatus["time"] = {"start": calendar.timegm(info[5].utctimetuple())}
                        clientStatus["time"]["lastrequest"] = calendar.timegm(info[6].utctimetuple())
                        clientsStatusList.append(clientStatus)
                    # Server status
                    serverStatus = {"pid": os.getpid()}
                    serverStatus["time"] = {"start": calendar.timegm(self.server.startTime.utctimetuple())}
                    serverStatus["state"] = self.server.state
                    counts = persist.count()
                    serverStatus["counts"] = {"total": counts[0]}
                    serverStatus["counts"]["succeeded"] = counts[1]
                    serverStatus["counts"]["inprogress"] = counts[2]
                    serverStatus["counts"]["available"] = counts[3]
                    serverStatus["counts"]["failed"] = counts[4]
                    serverStatus["counts"]["error"] = counts[5]
                    # Send status 
                    client.send({"command": "GIVE_STATUS", "clients": clientsStatusList, "server": serverStatus})
                    running = False
                    
                elif (command == "RM_CLIENTS"):
                    clientIDList = []
                    # Pick all disconnected clients to remove them
                    if (message["clientidlist"][0] == "+"):
                        for ID in clientsInfo.keys():
                            if (not clientsThreads[ID][0].is_alive()): clientIDList.append(ID)
                    # Get client IDs specified by the user 
                    else: clientIDList = [int(ID) for ID in message["clientidlist"]]
                    # Do remove
                    removeSuccess = []
                    removeError = []
                    for ID in clientIDList:
                        if (self.removeClient(ID)): removeSuccess.append(ID)
                        else: removeError.append(ID)
                    # Wait for alive threads to safely terminate
                    if (removeSuccess): 
                        with clientRemovedCondition: 
                            while any(ID in clientsInfo for ID in removeSuccess): clientRemovedCondition.wait()
                    # Send response to manager
                    client.send({"command": "RM_RET", "successlist": [str(ID) for ID in removeSuccess], "errorlist": [str(ID) for ID in removeError]})
                    running = False
                    
                elif (command == "RESET"):
                    statusName = message["status"]
                    if (statusName == "INPROGRESS") and (clientsInfo): 
                        client.send({"command": "RESET_RET", "fail": True, "reason": "It is not possible to reset INPROGRESS resources while there are clients connected."})
                    else:
                        resetCount = persist.reset(status[statusName])
                        client.send({"command": "RESET_RET", "fail": False, "count": resetCount})
                    running = False
                        
                elif (command == "SHUTDOWN"):
                    running = False
                    with shutdownLock:
                        if (self.server.state == "running"): 
                            self.server.state = "shuting down"
                        else: 
                            client.send({"command": "SD_RET", "fail": True, "reason": "Cannot perform action, server is %s." % ("already shuting down" if (self.server.state == "shuting down") else self.server.state)})
                            continue
                    logging.info("Finishing all clients to shut down...")
                    if (config["server"]["verbose"]): print "Finishing all clients to shut down..."
                    for ID in clientsInfo.keys(): self.removeClient(ID)
                    with clientRemovedCondition:
                        while (clientsInfo): clientRemovedCondition.wait()
                    client.send({"command": "SD_RET", "fail": False})
                    
            except Exception as error:
                logging.exception("Exception while processing a request from client %d. Execution of thread '%s' aborted." % (clientID, threading.current_thread().name))
                if (config["server"]["verbose"]): 
                    print "ERROR: %s" % str(error)
                    excType, excObj, excTb = sys.exc_info()
                    fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
                    print (excType, fileName, excTb.tb_lineno)
                running = False
    
    def finish(self):
        self.persist.close()
        for filter in self.parallelFilters: filter.close()
        for filter in self.sequentialFilters: filter.close()
        
        # If this is the last client finishing, free resources allocated by persistence and filters objects
        if (self.server.state != "running") and (threading.active_count() == 2):
            logging.info("Freeing allocated objects...")
            if (self.server.config["server"]["verbose"]): print "Freeing allocated objects..."
            self.persist.shutdown()
            for filter in self.parallelFilters: filter.shutdown()
            for filter in self.sequentialFilters: filter.shutdown()
            self.server.shutdown() 
        
    def removeClient(self, ID):
        with removeClientLock:
            # Client exists?
            if (ID in clientsInfo):
                # Client is running?
                if (clientsThreads[ID][0].is_alive()):
                    clientsThreads[ID][1].set()
                else:
                    del clientsInfo[ID]
                    logging.info("Client %d removed." % ID)
                    if (self.server.config["server"]["verbose"]): print "Client %d removed." % ID
                return True
            else:
                return False
                
    def threadedFilterWrapper(self, filter, resourceID, resourceInfo, outputList):
        data = filter.apply(resourceID, resourceInfo, None)
        outputList.append({"filter": filter.getName(), "order": None, "data": data})
                
    def applyFilters(self, resourceID, resourceInfo):
        parallelFilters = self.parallelFilters
        sequentialFilters = self.sequentialFilters
        filters = []
    
        # Start threaded filters
        filterThreads = []
        for filter in parallelFilters:
            t = threading.Thread(target=self.threadedFilterWrapper, args=(filter, resourceID, resourceInfo, filters))
            filterThreads.append(t)
            t.start()
        
        # Execute sequential filters
        data = {}
        for filter in sequentialFilters:
            data = filter.apply(resourceID, resourceInfo, data.copy())
            filters.append({"name": filter.getName(), "order": sequentialFilters.index(filter), "data": data})
            
        # Wait for threaded filters to finish
        for filter in filterThreads:
            filter.join()
        
        return (filters if filters else None)
                
                
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, configurationsDictionary):
        self.config = configurationsDictionary
        
        # Configure logging
        if (self.config["server"]["logging"]):
            logging.basicConfig(format="%(asctime)s %(module)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                                filename="server[%s%s].log" % (socket.gethostname(), self.config["global"]["connection"]["port"]), filemode="w", level=logging.INFO)
                                
        # Get persistence handler and filters classes
        self.PersistenceHandlerClass = getattr(persistence, self.config["persistence"]["handler"]["class"])
        self.FiltersClasses = [getattr(filters, filter["class"]) for filter in self.config["server"]["filter"]]
                                
        # Call SocketSever constructor
        SocketServer.TCPServer.__init__(self, (self.config["global"]["connection"]["address"], self.config["global"]["connection"]["port"]), ServerHandler)
    
    def run(self):
        self.startTime = datetime.now()
        self.state = "running"
        
        logging.info("Server ready. Waiting for connections...")
        if (self.config["server"]["verbose"]): print "Server ready. Waiting for connections..."
        self.serve_forever()
        
        if (self.state == "finishing"):
            logging.info("Server finished." )
            if (self.config["server"]["verbose"]): print "Server finished."
        else: 
            logging.info("Server manually shut down.")
            if (self.config["server"]["verbose"]): print "Server manually shut down."
            