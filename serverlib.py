# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import SocketServer
import threading
import json
import time
import timeit
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

# Timing variables
serverAggregatedTimes = {0: 0.0}
clientAggregatedTimes = {0: 0.0}
crawlerAggregatedTimes = {0: 0.0}
numTimingMeasures = {0: long(0)}
numCrawlingMeasures = {0: long(0)}

# Define synchronization objects for critical regions of the code
removeClientLock = threading.Lock()
clientFinishedCondition = threading.Condition()
shutdownLock = threading.Lock()


# ==================== Classes ====================
class ServerHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        global nextFreeID
        self.clientID = 0
        self.persist = None
    
        # Get network handler instance
        self.client = common.NetworkHandler(self.request)
        message = self.client.recv()
    
        with shutdownLock:
            if (self.server.state == "running"):
                self.connectionAccepted = True
                
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
                
                if (message["type"] == "client"):
                    # Set client ID and other informations
                    clientAddress = self.client.getaddress()
                    clientPid = message["processid"]
                    self.clientID = nextFreeID
                    nextFreeID += 1
                    clientsThreads[self.clientID ] = (threading.current_thread(), threading.Event())
                    clientsInfo[self.clientID ] = [clientAddress, clientPid, None, None, -1, datetime.now(), None]
                    self.server.echo.default("New client connected: %d" % self.clientID)

                self.client.send({"command": "ACCEPTED", "clientid": self.clientID})
            else:
                self.connectionAccepted = False
                self.client.send({"command": "REFUSED", "reason": "Connection refused, server is %s." % self.server.state})

    def handle(self):
        # Declare global variables
        global serverAggregatedTimes
        global clientAggregatedTimes
        global crawlerAggregatedTimes
        global numTimingMeasures
        global numCrawlingMeasures
    
        # Define some local variables
        config = self.server.config
        echo = self.server.echo
        client = self.client
        clientID = self.clientID
        persist = self.persist
        status = persistence.StatusCodes()
        
        # Set timing variables initial values
        serverAggregatedTimes[clientID] = 0.0
        clientAggregatedTimes[clientID] = 0.0
        crawlerAggregatedTimes[clientID] = 0.0
        numTimingMeasures[clientID] = long(0)
        numCrawlingMeasures[clientID] = long(0)
    
        # Start to handle
        running = True
        while (self.connectionAccepted and running):
            try: 
                startClientTime = timeit.default_timer()
                startCrawlerTime = timeit.default_timer()
                
                message = client.recv()
                
                endClientTime = timeit.default_timer()
                endCrawlerTime = timeit.default_timer()
                startServerTime = timeit.default_timer()
                
                # Stop thread execution if the connection has been interrupted
                if (not message): 
                    clientResourceKey = clientsInfo[clientID][2]
                    echo.default("Connection to client %d has been abruptly closed." % clientID, "ERROR")
                    persist.update(clientResourceKey, status.ERROR, None)
                    running = False
                    continue

                command = message["command"]
                
                if (command == "GET_ID"):
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
                            (resourceKey, resourceID, resourceInfo) = persist.select()
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
                                            echo.default("Task done, finishing clients...")
                                            self.server.state = "finishing"
                                            for ID in clientsInfo.keys(): self.removeClient(ID)
                                    tryagain = True
                        # If the client has been removed, finish it
                        else:
                            del clientsInfo[clientID]
                            if (self.server.state == "running"):
                                client.send({"command": "FINISH", "reason": "removed"})
                                echo.default("Client %d removed." % clientID)
                            else:
                                if (self.server.state == "finishing"): 
                                    client.send({"command": "FINISH", "reason": "task done"})
                                elif (self.server.state == "shuting down"): 
                                    client.send({"command": "FINISH", "reason": "shut down"})
                                echo.default("Client %d finished." % clientID)
                            running = False
                    
                elif (command == "DONE_ID"):
                    clientResourceKey = clientsInfo[clientID][2]
                    clientResourceID = clientsInfo[clientID][3]
                    clientResourceInfo = message["resourceinfo"]
                    clientExtraInfo = message["extrainfo"]
                    clientNewResources = message["newresources"]
                    self.callbackFilters(clientResourceID, clientResourceInfo, clientExtraInfo, clientNewResources)
                    if (config["global"]["feedback"]): persist.insert(clientNewResources)
                    persist.update(clientResourceKey, status.SUCCEEDED, clientResourceInfo)
                    client.send({"command": "DONE_RET"})
                            
                elif (command == "EXCEPTION"):
                    clientResourceKey = clientsInfo[clientID][2]
                    clientResourceID = clientsInfo[clientID][3]
                    if (message["type"] == "fail"):
                        echo.default("Client %s reported fail for resource %s." % (clientID, clientResourceID), "WARNING")
                        persist.update(clientResourceKey, status.FAILED, None)
                        client.send({"command": "EXCEPTION_RET"})
                    elif (message["type"] == "error"):
                        echo.default("Client %s reported error for resource %s. Connection closed." % (clientID, clientResourceID), "ERROR")
                        persist.update(clientResourceKey, status.ERROR, None)
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
                        clientStatus["time"] = {"start": info[5]}
                        clientStatus["time"]["lastrequest"] = info[6]
                        clientStatus["time"]["agrserver"] = serverAggregatedTimes[ID]
                        clientStatus["time"]["agrclient"] = clientAggregatedTimes[ID]
                        clientStatus["time"]["agrcrawler"] = crawlerAggregatedTimes[ID]
                        clientStatus["time"]["avgserver"] = serverAggregatedTimes[ID] / numTimingMeasures[ID]
                        clientStatus["time"]["avgclient"] = clientAggregatedTimes[ID] / numTimingMeasures[ID]
                        clientStatus["time"]["avgcrawler"] = crawlerAggregatedTimes[ID] / numCrawlingMeasures[ID] if (numCrawlingMeasures[ID] > 0) else 0
                        clientsStatusList.append(clientStatus)
                    # Server status
                    serverStatus = {"pid": os.getpid()}
                    serverStatus["state"] = self.server.state
                    counts = persist.count()
                    serverStatus["counts"] = {"total": counts[0]}
                    serverStatus["counts"]["succeeded"] = counts[1]
                    serverStatus["counts"]["inprogress"] = counts[2]
                    serverStatus["counts"]["available"] = counts[3]
                    serverStatus["counts"]["failed"] = counts[4]
                    serverStatus["counts"]["error"] = counts[5]
                    serverStatus["time"] = {"start": self.server.startTime}
                    serverStatus["time"]["current"] = datetime.now()
                    # Send status 
                    client.send({"command": "GIVE_STATUS", "clients": clientsStatusList, "server": serverStatus})
                    running = False
                    
                elif (command == "RM_CLIENTS"):
                    clientIDs = set(message["clientids"])
                    clientNames = message["clientnames"]
                    # Get IDs of clients specified by name or IDs corresponding to the keywords 'all' and 'disconnected'
                    for (ID, info) in clientsInfo.items():
                        if (("all" in clientNames) or
                            (info[0][0] in clientNames) or 
                            ((not clientsThreads[ID][0].is_alive()) and ("disconnected" in clientNames))): 
                            clientIDs.add(ID)
                    # Do remove
                    removeSuccess = []
                    removeError = []
                    for ID in clientIDs:
                        if (self.removeClient(ID)): removeSuccess.append(ID)
                        else: removeError.append(ID)
                    # Wait for alive threads to safely terminate
                    if (removeSuccess): 
                        with clientFinishedCondition: 
                            while any(ID in clientsInfo for ID in removeSuccess): clientFinishedCondition.wait()
                    # Send response to manager
                    client.send({"command": "RM_RET", "successlist": [str(ID) for ID in removeSuccess], "errorlist": [str(ID) for ID in removeError]})
                    running = False
                    
                elif (command == "RESET"):
                    statusName = message["status"]
                    if ((statusName == "INPROGRESS") or (statusName == "SUCCEEDED")) and (clientsInfo): 
                        client.send({"command": "RESET_RET", "fail": True, "reason": "It is not possible to reset %s resources while there are clients connected." % statusName})
                    else:
                        resetCount = persist.reset(getattr(status, statusName))
                        client.send({"command": "RESET_RET", "fail": False, "count": resetCount})
                    running = False
                        
                elif (command == "SHUTDOWN"):
                    running = False
                    with shutdownLock:
                        if (self.server.state == "running"):
                            echo.default("Finishing all clients to shut down...")
                            self.server.state = "shuting down"
                            for ID in clientsInfo.keys(): self.removeClient(ID)
                        else: 
                            client.send({"command": "SD_RET", "state": "failed", "reason": "Cannot perform action, server is %s." % ("already shuting down" if (self.server.state == "shuting down") else self.server.state)})
                            continue
                    with clientFinishedCondition:
                        self.client.send({"command": "SD_RET", "state": "sdclients", "remaining": len(clientsInfo)})
                        while (clientsInfo): 
                            clientFinishedCondition.wait()
                            self.client.send({"command": "SD_RET", "state": "sdclients", "remaining": len(clientsInfo)})
                
                endServerTime = timeit.default_timer()
                serverAggregatedTimes[clientID] += (endServerTime - startServerTime)
                clientAggregatedTimes[clientID] += (endClientTime - startClientTime)
                numTimingMeasures[clientID] += 1
                if (command == "DONE_ID"):
                    crawlerAggregatedTimes[clientID] += (endCrawlerTime - startCrawlerTime)
                    numCrawlingMeasures[clientID] += 1
                    
            except Exception as error:
                echo.exception("Exception while processing a request from client %d. Execution of thread '%s' aborted." % (clientID, threading.current_thread().name))
                # if (config["server"]["verbose"]): 
                    # print "ERROR: %s" % str(error)
                    # excType, excObj, excTb = sys.exc_info()
                    # fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
                    # print (excType, fileName, excTb.tb_lineno)
                running = False
    
    def finish(self):
        if (self.connectionAccepted):
            for filter in self.parallelFilters: filter.finish()
            for filter in self.sequentialFilters: filter.finish()
            self.persist.finish()
            
            # Free resources allocated by persistence and filter objects after all clients have finished
            if (((self.server.state == "finishing") and (not clientsInfo)) or
                ((self.server.state == "shuting down") and (self.clientID == 0))):
                self.server.echo.default("Shuting down filters...")
                if (self.clientID == 0): self.client.send({"command": "SD_RET", "state": "sdfilters"})
                for filter in self.parallelFilters: filter.shutdown()
                for filter in self.sequentialFilters: filter.shutdown()
                self.server.echo.default("Shuting down persistence handler...")
                if (self.clientID == 0): self.client.send({"command": "SD_RET", "state": "sdpersistence"})
                self.persist.shutdown()
                self.server.shutdown()
                if (self.clientID == 0): self.client.send({"command": "SD_RET", "state": "complete"})
                
            with clientFinishedCondition: clientFinishedCondition.notify_all()
        
    def removeClient(self, ID):
        with removeClientLock:
            # Client exists?
            if (ID in clientsInfo):
                # Client is running?
                if (clientsThreads[ID][0].is_alive()):
                    clientsThreads[ID][1].set()
                else:
                    del clientsInfo[ID]
                    self.server.echo.default("Client %d removed." % ID)
                return True
            return False
                
    def threadedFilterApplyWrapper(self, filter, resourceID, resourceInfo, outputList):
        data = filter.apply(resourceID, deepcopy(resourceInfo), None)
        outputList.append({"filter": filter.name(), "order": None, "data": data})
        
    def threadedFilterCallbackWrapper(self, filter, resourceID, resourceInfo, newResources, extraInfo):
        filter.callback(resourceID, deepcopy(resourceInfo), deepcopy(newResources), deepcopy(extraInfo))
                
    def applyFilters(self, resourceID, resourceInfo):
        parallelFilters = self.parallelFilters
        sequentialFilters = self.sequentialFilters
        filtersData = []
    
        # Start threaded filters
        filterThreads = []
        for filter in parallelFilters:
            t = threading.Thread(target=self.threadedFilterApplyWrapper, args=(filter, resourceID, resourceInfo, filtersData))
            filterThreads.append(t)
            t.start()
        
        # Execute sequential filters
        extraInfo = {}
        for filter in sequentialFilters:
            data = filter.apply(resourceID, deepcopy(resourceInfo), extraInfo)
            filtersData.append({"name": filter.name(), "order": sequentialFilters.index(filter), "data": data})
            
        # Wait for threaded filters to finish
        for filter in filterThreads:
            filter.join()
        
        return (filtersData if (filtersData) else None)
        
    def callbackFilters(self, resourceID, resourceInfo, extraInfo, newResources):
        parallelFilters = self.parallelFilters
        sequentialFilters = self.sequentialFilters
    
        # Start threaded filters
        filterThreads = []
        for filter in parallelFilters:
            t = threading.Thread(target=self.threadedFilterCallbackWrapper, args=(filter, resourceID, resourceInfo, newResources, extraInfo))
            filterThreads.append(t)
            t.start()
        
        # Execute sequential filters
        extraInfoRef = {}
        for filter in sequentialFilters:
            extraInfoRef["original"] = deepcopy(extraInfo)
            filter.callback(resourceID, resourceInfo, newResources, extraInfoRef)
            
        # Wait for threaded filters to finish
        for filter in filterThreads:
            filter.join()
        
            
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, configurationsDictionary):
        self.config = configurationsDictionary
        
        # Configure echoing
        self.echo = common.EchoHandler(self.config["server"], "server[%s%s].log" % (socket.gethostname(), self.config["global"]["connection"]["port"]))
        
        # Get persistence handler and filters classes
        self.PersistenceHandlerClass = getattr(persistence, self.config["persistence"]["handler"]["class"])
        self.FiltersClasses = [getattr(filters, filter["class"]) for filter in self.config["server"]["filter"]]
                                
        # Call SocketSever constructor
        SocketServer.TCPServer.__init__(self, (self.config["global"]["connection"]["address"], self.config["global"]["connection"]["port"]), ServerHandler)
    
    def run(self):
        self.startTime = datetime.now()
        self.state = "running"
        
        self.echo.default("Server ready. Waiting for connections...")
        self.serve_forever()
        
        if (self.state == "finishing"): self.echo.default("Server finished." )
        else: self.echo.default("Server manually shut down.")
            