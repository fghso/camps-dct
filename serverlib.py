# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import SocketServer
import threading
import json
import time
import timeit
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

# Timing variables
serverAggregatedTimes = {0: 0.0}
clientAggregatedTimes = {0: 0.0}
crawlerAggregatedTimes = {0: 0.0}
numTimingMeasures = {0: long(0)}
numCrawlingMeasures = {0: long(0)}

# Define synchronization objects for critical regions of the code
removeClientLock = threading.Lock()
clientRemovedCondition = threading.Condition()
shutdownLock = threading.Lock()


# ==================== Classes ====================
class ServerHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        # Get network handler instance
        self.client = common.NetworkHandler(self.request)
    
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
        # Declare global variables
        global nextFreeID
        global serverAggregatedTimes
        global clientAggregatedTimes
        global crawlerAggregatedTimes
        global numTimingMeasures
        global numCrawlingMeasures
    
        # Define some local variables
        config = self.server.config
        echo = self.server.echo
        client = self.client
        persist = self.persist
        status = persistence.StatusCodes()
    
        # Start to handle
        clientID = 0
        running = True
        while (running):
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
                    serverAggregatedTimes[clientID] = 0.0
                    clientAggregatedTimes[clientID] = 0.0
                    crawlerAggregatedTimes[clientID] = 0.0
                    numTimingMeasures[clientID] = long(0)
                    numCrawlingMeasures[clientID] = long(0)
                    client.send({"command": "GIVE_LOGIN", "fail": False, "clientid": clientID})
                    echo.default("New client connected: %d" % clientID)
                
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
                            with clientRemovedCondition: clientRemovedCondition.notify_all()
                            if (self.server.state == "running"):
                                client.send({"command": "FINISH", "reason": "removed"})
                                echo.default("Client %d removed." % clientID)
                            else:
                                if (self.server.state == "finishing"): client.send({"command": "FINISH", "reason": "task done"})
                                if (self.server.state == "shuting down"): client.send({"command": "FINISH", "reason": "shut down"})
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
                    persist.update(clientResourceKey, status.SUCCEDED, clientResourceInfo)
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
                        clientStatus["time"] = {"start": calendar.timegm(info[5].utctimetuple())}
                        clientStatus["time"]["lastrequest"] = calendar.timegm(info[6].utctimetuple())
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
                    serverStatus["time"] = {"start": calendar.timegm(self.server.startTime.utctimetuple())}
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
                        resetCount = persist.reset(getattr(status, statusName))
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
                    echo.default("Finishing all clients to shut down...")
                    for ID in clientsInfo.keys(): self.removeClient(ID)
                    with clientRemovedCondition:
                        while (clientsInfo): clientRemovedCondition.wait()
                    client.send({"command": "SD_RET", "fail": False})
                    
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
        self.persist.close()
        for filter in self.parallelFilters: filter.close()
        for filter in self.sequentialFilters: filter.close()
        
        # If this is the last client finishing, free resources allocated by persistence and filters objects
        if (self.server.state != "running") and (threading.active_count() == 2):
            self.server.echo.default("Freeing allocated objects...")
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
                    self.server.echo.default("Client %d removed." % ID)
                return True
            else:
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
            