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


# ==================== Global variables ====================
# Dictionary to store information lists about each client:
#   [network address, process identification (PID), ID of the resource being collected, 
#    number of resources already collected, collection start time and last GET_ID requested time] 
clientsInfo = {} 

# Store a reference for the thread running the client and an event to interrupt its execution
clientsThreads = {}

# Define the next ID to give to a new client
nextFreeID = 1

# Flag to indicate wheter the server is running or no
shutingDown = False

# Define synchronization locks for critical regions of the code
nextFreeIDLock = threading.Lock()
getIDLock = threading.Lock()
removeClientLock = threading.Lock()
shutdownLock = threading.Lock()


# ==================== Classes ====================
class ServerHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        # Get persistence instance
        self.persist = self.server.PersistenceHandlerClass(self.server.config)
        # Get filters instances
        self.parallelFilters = [FilterClass(filterName) for (FilterClass, filterName) in self.server.ParallelFiltersClasses]
        self.sequentialFilters = [FilterClass(filterName) for (FilterClass, filterName) in self.server.SequentialFiltersClasses]

    def handle(self):
        # Declare global variables
        global nextFreeID
        global shutingDown
    
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
                    clientResourceID = clientsInfo[clientID][2]
                    logging.error("Connection to client %d has been abruptly closed." % clientID)
                    if (config["server"]["verbose"]): print "ERROR: Connection to client %d has been abruptly closed." % clientID
                    persist.update(clientResourceID, status["ERROR"], None)
                    running = False
                    continue

                command = message["command"]
                
                if (command == "GET_LOGIN"):
                    with nextFreeIDLock:
                        clientID = nextFreeID
                        nextFreeID += 1
                    clientAddress = client.getaddress()
                    clientPid = message["processid"]
                    clientsInfo[clientID] = [clientAddress, clientPid, None, -1, datetime.now(), None]
                    clientsThreads[clientID] = (threading.current_thread(), threading.Event())
                    client.send({"command": "GIVE_LOGIN", "clientid": clientID})
                    logging.info("New client connected: %d" % clientID)
                    if (config["server"]["verbose"]): print "New client connected: %d" % clientID
                
                elif (command == "GET_ID"):
                    clientStopEvent = clientsThreads[clientID][1]
                    clientsInfo[clientID][2] = None
                    clientsInfo[clientID][3] += 1
                    clientsInfo[clientID][5] = datetime.now()
                    tryagain = True
                    while (tryagain):
                        tryagain = False
                        # If the client hasn't been removed, check resource availability
                        if (not clientStopEvent.is_set()):
                            with getIDLock:
                                (resourceID, resourceInfo) = persist.select()
                                if (resourceID): persist.update(resourceID, status["INPROGRESS"], None)
                            # If there is a resource available, send ID to client
                            if (resourceID):
                                clientsInfo[clientID][2] = resourceID
                                clientsInfo[clientID][5] = datetime.now()
                                filters = self.applyFilters(resourceID, resourceInfo)
                                client.send({"command": "GIVE_ID", "resourceid": resourceID, "filters": filters})
                            else:
                                # If there isn't resources available and loopforever is true, wait some time and check again
                                if (config["server"]["loopforever"]): 
                                    time.sleep(5)
                                    tryagain = True
                                # If there isn't resources available and loopforever is false, 
                                # finish client and signal server to shut down
                                else:
                                    client.send({"command": "FINISH"})
                                    del clientsInfo[clientID]
                                    notShutingDown = shutdownLock.acquire(False)
                                    if (notShutingDown):
                                        try:
                                            shutingDown = True
                                            logging.info("Task done, finishing clients and server... \nClient %d finished." % clientID)
                                            if (config["server"]["verbose"]): print "Task done, finishing clients and server... \nClient %d finished." % clientID
                                            for ID in clientsThreads.iterkeys(): 
                                                if (ID != clientID): clientsThreads[ID][0].join()
                                            self.server.shutdown()
                                            logging.info("Server finished." )
                                            if (config["server"]["verbose"]): print "Server finished."
                                        except: 
                                            shutingDown = False
                                            shutdownLock.release()
                                    else: 
                                        logging.info("Client %d finished." % clientID)
                                        if (config["server"]["verbose"]): print "Client %d finished." % clientID 
                                    running = False
                        # If the client has been removed, kill it
                        else:
                            client.send({"command": "KILL"})
                            del clientsInfo[clientID]
                            logging.info("Client %d removed." % clientID)
                            if (config["server"]["verbose"]): print "Client %d removed." % clientID
                            running = False
                    
                elif (command == "DONE_ID"):
                    clientResourceID = clientsInfo[clientID][2]
                    if (message["fail"]):
                        persist.update(clientResourceID, status["FAILED"], None)
                        client.send({"command": "DONE_RET", "fail": False})
                    else:
                        insertErrors = []
                        # If feedback is enabled, try to insert the new resources sent by the client
                        if (config["global"]["feedback"]):
                            clientNewResources = message["newresources"]
                            for resource in clientNewResources:
                                if (not persist.insert(resource[0], resource[1])):
                                    insertErrors.append(str(resource[0]))
                        # Report failed resource IDs in case of insertion errors            
                        if (insertErrors):
                            if (len(insertErrors) > 1): 
                                logging.error("Failed to insert the new resources %s sent by client %s after collect resource %s." % (" and ".join((", ".join(insertErrors[:-1]), insertErrors[-1])), clientID, clientResourceID))
                            else: 
                                logging.error("Failed to insert the new resource %s sent by client %s after collect resource %s." % (insertErrors[0], clientID, clientResourceID))
                            persist.update(clientResourceID, status["FAILED"], None)
                            client.send({"command": "DONE_RET", "fail": True, "inserterrors": insertErrors})
                        # Report everything is OK    
                        else: 
                            clientResourceInfo = message["resourceinfo"]
                            persist.update(clientResourceID, status["SUCCEDED"], clientResourceInfo)
                            client.send({"command": "DONE_RET", "fail": False})
                            
                elif (command == "ERROR"):
                    clientResourceID = clientsInfo[clientID][2]
                    if (message["type"] == "critical"):
                        logging.error("Client %s reported critical error for resource %s. Connection closed." % (clientID, clientResourceID))
                        if (config["server"]["verbose"]): print "ERROR: Critical error on client %s, connection closed." % clientID
                        persist.update(clientResourceID, status["ERROR"], None)
                        running = False
                    elif (message["type"] == "cleanup"):
                        logging.error("Client %s reported clean up error for resource %s." % (clientID, clientResourceID))
                        persist.update(clientResourceID, status["ERROR"], None)
                        client.send({"command": "ERROR_RET"})
                    
                elif (command == "GET_STATUS"):
                    # Clients status
                    clientsStatusList = []
                    for (ID, info) in clientsInfo.items():
                        clientThreadState = ((-1 if clientsThreads[ID][1].is_set() else 0) if clientsThreads[ID][0].is_alive() else -2)
                        clientStatus =  {"clientid": ID}
                        clientStatus["threadstate"] = clientThreadState
                        clientStatus["address"] = info[0]
                        clientStatus["pid"] = info[1]
                        clientStatus["resourceid"] = info[2]
                        clientStatus["amount"] = info[3]
                        clientStatus["time"] = {"start": calendar.timegm(info[4].utctimetuple())}
                        clientStatus["time"]["lastrequest"] = calendar.timegm(info[5].utctimetuple())
                        clientsStatusList.append(clientStatus)
                    # Server status
                    serverStatus = {"pid": os.getpid()}
                    serverStatus["time"] = {"start": calendar.timegm(self.server.startTime.utctimetuple())}
                    serverStatus["shutingdown"] = shutingDown
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
                        while any(ID in clientsInfo for ID in removeSuccess): pass
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
                    # Interrupt all active clients and mark resources requested by inactive 
                    # clients as not collected. After that, shut down server
                    notShutingDown = shutdownLock.acquire(False)
                    if (notShutingDown):
                        try:
                            shutingDown = True
                            logging.info("Removing all clients to shut down...")
                            if (config["server"]["verbose"]): print "Removing all clients to shut down..."
                            for ID in clientsInfo.keys(): self.removeClient(ID)
                            while (clientsInfo): pass
                            self.server.shutdown()    
                            client.send({"command": "SD_RET", "fail": False})
                            logging.info("Server manually shut down.")
                            if (config["server"]["verbose"]): print "Server manually shut down."
                        except:
                            shutingDown = False
                            shutdownLock.release()
                    else:
                        client.send({"command": "SD_RET", "fail": True, "reason": "Cannot perform action, server is already shuting down."})
                    running = False
                    
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
                                filename="server[%s%s].log" % (socket.gethostname(), self.config["global"]["connection"]["port"]), filemode="w", level=logging.DEBUG)
                                
        # Add persistence handler
        self.PersistenceHandlerClass = getattr(persistence, self.config["persistence"]["handler"]["class"])
        
        # Add filters
        self.ParallelFiltersClasses = []
        self.SequentialFiltersClasses = []
        for filter in self.config["server"]["filter"]:
            if (filter["enable"]):
                FilterClass = getattr(filters, filter["class"])
                filterName = filter["name"]
                if (filter["parallel"]): self.ParallelFiltersClasses.append((FilterClass, filterName))
                else: self.SequentialFiltersClasses.append((FilterClass, filterName))
                
        # Call SocketSever constructor
        SocketServer.TCPServer.__init__(self, (self.config["global"]["connection"]["address"], self.config["global"]["connection"]["port"]), ServerHandler)
    
    def start(self):
        logging.info("Server ready. Waiting for connections...")
        if (self.config["server"]["verbose"]): print "Server ready. Waiting for connections..."
        self.startTime = datetime.now()
        self.serve_forever()
