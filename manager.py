#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import json
import argparse
import common
from datetime import datetime


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False, description="Send action commands to be performed by the server or retrieve status information. If none of the optional arguments are given, show basic status information.")
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-s", "--status", choices=["raw", "basic", "extended"], help="show status information")
parser.add_argument("-r", "--remove", metavar="clientID", nargs="+", help="remove clients from the server's list. Multiple client IDs can be given, separated by spaces. To remove all disconnected clients, write '+' as the ID")
parser.add_argument("--reset", choices=["inprogress", "failed", "error"], help="make available the resources with the specified status")
parser.add_argument("--shutdown", action="store_true", help="remove all clients from the server's list and shutdown server")
args = parser.parse_args()

# Load configurations
config = common.loadConfig(args.configFilePath)

# Connect to server
try:
    server = common.NetworkHandler()
    server.connect(config["global"]["connection"]["address"], config["global"]["connection"]["port"])
except:
    sys.exit("ERROR: It was not possible to connect to server at %s:%s." % (config["global"]["connection"]["address"], config["global"]["connection"]["port"]))

# Remove client
if (args.remove):
    server.send({"command": "RM_CLIENTS", "clientidlist": args.remove})
    message = server.recv()
    server.close()
    
    removeSuccess = message["successlist"]
    removeError = message["errorlist"]
    if (not removeSuccess) and (not removeError): print "No disconnected client found to remove."
    if (len(removeSuccess) > 1): 
        print "Clients %s successfully removed." % " and ".join((", ".join(removeSuccess[:-1]), removeSuccess[-1]))
    elif (removeSuccess): 
        print "Client %s successfully removed." % removeSuccess[0]
    if (len(removeError) > 1): 
        print "ERROR: Clients %s do not exist or have already been removed." % " and ".join((", ".join(removeError[:-1]), removeError[-1]))
    elif (removeError): 
        print "ERROR: Client %s does not exist or has already been removed." % removeError[0]
    
# Reset resources status
elif (args.reset):   
    server.send({"command": "RESET", "status": args.reset.upper()})
    message = server.recv()
    server.close()    
    
    if (message["fail"]):
        print "ERROR: %s" % message["reason"]
    else:
        if (message["count"]): print "Resources with %s status successfully reseted." % args.reset.upper()
        else: print "No resources with %s status found." % args.reset.upper()
    
# Shut down server
elif (args.shutdown):   
    server.send({"command": "SHUTDOWN"})
    message = server.recv()
    server.close()
    
    if (message["fail"]): print "ERROR: %s" % message["reason"]
    else: print "Server successfully shut down."
        
# Show status
else:
    server.send({"command": "GET_STATUS"})
    message = server.recv()
    serverAddress = server.getaddress()
    server.close()
    
    clientsStatusList = message["clients"]
    serverStatus = message["server"]
    serverStatus["time"]["start"] = datetime.utcfromtimestamp(serverStatus["time"]["start"])
    
    # Raw status
    if (args.status == "raw"):
        status = "\n" + (" Status ").center(50, ':') + "\n\n"
        status += "  Server:\n"
        status += str("    [address, port, pid, state, start, total, succeeded, inprogress, available, failed, error]\n    ")
        status += str([serverAddress[1], serverAddress[2], serverStatus["pid"], 
                        serverStatus["state"],
                        serverStatus["time"]["start"].strftime("%d/%m/%Y %H:%M:%S"),
                        serverStatus["counts"]["total"], serverStatus["counts"]["succeeded"], 
                        serverStatus["counts"]["inprogress"], serverStatus["counts"]["available"], 
                        serverStatus["counts"]["failed"], serverStatus["counts"]["error"]])
        status += "\n\n  Clients:\n"
        if (clientsStatusList): 
            status += str("    [id, state, hostname, address, port, pid, start, lastrequest, meanservertime, meanclienttime, meancrawlertime, resource, amount]\n    ")
        else: 
            status += "    No client connected right now.\n"
        for clientStatus in clientsStatusList:
            clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
            clientStatus["time"]["start"] = datetime.utcfromtimestamp(clientStatus["time"]["start"])
            clientStatus["time"]["lastrequest"] = datetime.utcfromtimestamp(clientStatus["time"]["lastrequest"])
            status += str([clientStatus["clientid"], clientStatus["threadstate"], str(clientStatus["address"][0]), 
                        str(clientStatus["address"][1]), clientStatus["address"][2], clientStatus["pid"],
                        clientStatus["time"]["start"].strftime("%d/%m/%Y %H:%M:%S"), 
                        clientStatus["time"]["lastrequest"].strftime("%d/%m/%Y %H:%M:%S"),
                        "%08.5f" % clientStatus["time"]["meanserver"],
                        "%08.5f" % clientStatus["time"]["meanclient"],                        
                        "%08.5f" % clientStatus["time"]["meancrawler"],
                        clientStatus["resourceid"] if (clientStatus["resourceid"]) else "waiting", 
                        clientStatus["amount"]])
            status += "\n    "
        status += "\n" + (" Status ").center(50, ':') + "\n"
    # Extended status
    elif (args.status == "extended"):
        status = "\n" + (" Status ".center(50, ':')) + "\n\n"
        if (clientsStatusList): 
            for clientStatus in clientsStatusList:
                clientStatus["clientid"] = "#%d" % clientStatus["clientid"]
                clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
                clientStatus["time"]["start"] = datetime.utcfromtimestamp(clientStatus["time"]["start"])
                clientStatus["time"]["lastrequest"] = datetime.utcfromtimestamp(clientStatus["time"]["lastrequest"])
                elapsedTime = datetime.now() - clientStatus["time"]["start"]
                elapsedMinSec = divmod(elapsedTime.seconds, 60)
                elapsedHoursMin = divmod(elapsedMinSec[0], 60)
                status += "  %3s %s %s (%s:%s/%s): %s since %s [%d processed in %s]\n" % (
                            clientStatus["clientid"], 
                            clientStatus["threadstate"], 
                            clientStatus["address"][0], 
                            clientStatus["address"][1], 
                            clientStatus["address"][2], 
                            clientStatus["pid"], 
                            "working on %s" % clientStatus["resourceid"] if (clientStatus["resourceid"]) else "waiting for new resource", 
                            clientStatus["time"]["lastrequest"].strftime("%d/%m/%Y %H:%M:%S"), 
                            clientStatus["amount"], 
                            #"" if (clientStatus["amount"] == 1) else "s",
                            "%02d:%02d:%02d" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1])
                        )
        else:
            status += "  No client connected right now.\n"
        status += "\n  " + (" Session Info ").center(46, '=') + "\n\n"
        elapsedTime = datetime.now() - serverStatus["time"]["start"]
        elapsedMinSec = divmod(elapsedTime.seconds, 60)
        elapsedHoursMin = divmod(elapsedMinSec[0], 60)
        clientsTotal = float(len(clientsStatusList))
        connectedClients = float(len([client for client in clientsStatusList if client["threadstate"] == " "]))
        disconnectedClients = float(len([client for client in clientsStatusList if client["threadstate"] == "+"]))
        removingClients = float(len([client for client in clientsStatusList if client["threadstate"] == "-"]))
        connectedClientsPercent = ((connectedClients / clientsTotal) * 100) if (clientsTotal > 0) else 0.0
        disconnectedClientsPercent = ((disconnectedClients / clientsTotal) * 100) if (clientsTotal > 0) else 0.0
        removingClientsPercent = ((removingClients / clientsTotal) * 100) if (clientsTotal > 0) else 0.0
        resourcesTotal = float(serverStatus["counts"]["total"])
        resourcesSucceeded = float(serverStatus["counts"]["succeeded"])
        resourcesInProgress = float(serverStatus["counts"]["inprogress"])
        resourcesAvailable = float(serverStatus["counts"]["available"])
        resourcesFailed = float(serverStatus["counts"]["failed"])
        resourcesError = float(serverStatus["counts"]["error"])
        resourcesProcessed = resourcesSucceeded + resourcesFailed + resourcesError
        resourcesSucceededPercent = ((resourcesSucceeded / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        resourcesInProgressPercent = ((resourcesInProgress / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        resourcesAvailablePercent = ((resourcesAvailable / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        resourcesFailedPercent = ((resourcesFailed / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        resourcesErrorPercent = ((resourcesError / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        resourcesProcessedPercent = ((resourcesProcessed / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        meanServerTime = sum([clientStatus["time"]["meanserver"] for clientStatus in clientsStatusList])
        meanServerMinSec = divmod(meanServerTime / clientsTotal, 60) if (clientsTotal > 0) else (0,0)
        meanServerHoursMin = divmod(meanServerMinSec[0], 60)
        meanClientTime = sum([clientStatus["time"]["meanclient"] for clientStatus in clientsStatusList])
        meanClientMinSec = divmod(meanClientTime/ clientsTotal, 60) if (clientsTotal > 0) else (0,0)
        meanClientHoursMin = divmod(meanClientMinSec[0], 60)
        meanCrawlerTime = sum([clientStatus["time"]["meancrawler"] for clientStatus in clientsStatusList])
        meanCrawlerMinSec = divmod(meanCrawlerTime / clientsTotal, 60) if (clientsTotal > 0) else (0,0)
        meanCrawlerHoursMin = divmod(meanCrawlerMinSec[0], 60)
        numResourcesProcessed = float(sum([clientStatus["amount"] for clientStatus in clientsStatusList]))
        meanResourcesPerclient = numResourcesProcessed / clientsTotal if (clientsTotal > 0) else 0.0
        meanResourcesPerSec = numResourcesProcessed / elapsedTime.seconds
        meanTimePerResource = elapsedTime.seconds / numResourcesProcessed if (numResourcesProcessed > 0) else 0.0
        meanResourceMinSec = divmod(meanTimePerResource, 60)
        meanResourceHoursMin = divmod(meanResourceMinSec[0], 60)
        estimatedTimeToFinish = meanTimePerResource * resourcesTotal
        estimatedMinSec = divmod(estimatedTimeToFinish, 60)
        estimatedHoursMin = divmod(estimatedMinSec[0], 60)
        status += "    Server state: %s\n" % serverStatus["state"]
        status += "      Server address: %s (%s:%s/%s)\n" % (serverAddress[0], serverAddress[1], serverAddress[2], serverStatus["pid"])
        status += "      Server uptime: %s\n" % ("%02d:%02d:%02d" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1]))
        status += "      Mean server time: %s\n" % ("%02d:%02d:%08.5f" % (meanServerHoursMin[0],  meanServerHoursMin[1], meanServerMinSec[1]))
        status += "      Mean client time: %s\n" % ("%02d:%02d:%08.5f" % (meanClientHoursMin[0],  meanClientHoursMin[1], meanClientMinSec[1]))
        status += "      Mean crawler time: %s\n" % ("%02d:%02d:%08.5f" % (meanCrawlerHoursMin[0],  meanCrawlerHoursMin[1], meanCrawlerMinSec[1]))
        status += "      Estimated time to finish: %s\n" % ("%02d:%02d:%02d" % (estimatedHoursMin[0],  estimatedHoursMin[1], estimatedMinSec[1]))
        status += "    Total number of clients: %d\n" % clientsTotal
        status += "      Connected clients: %d (%.2f%%)\n" % (connectedClients, connectedClientsPercent)
        status += "      Disconnected clients: %d (%.2f%%)\n" % (disconnectedClients, disconnectedClientsPercent)
        status += "      Clients being removed: %d (%.2f%%)\n" % (removingClients, removingClientsPercent)
        status += "    Number of resources processed: %d\n" % numResourcesProcessed
        status += "      Mean resources per client: %.2f\n" % meanResourcesPerclient
        status += "      Mean resources per time unit: %.2f/h, %.2f/m, %.2f/s\n" % (meanResourcesPerSec * 3600, meanResourcesPerSec * 60, meanResourcesPerSec)
        status += "      Mean time per resource: %02d:%02d:%08.5f\n" % (meanResourceHoursMin[0],  meanResourceHoursMin[1], meanResourceMinSec[1])
        status += "\n  " + (" Global Info ").center(46, '=') + "\n\n"
        status += "    Total number of resources: %d\n" % resourcesTotal
        status += "    Number of resources processed: %d (%.5f%%)\n" % (resourcesProcessed, resourcesProcessedPercent)
        status += "      Succeeded: %d (%.5f%%)\n" % (resourcesSucceeded, resourcesSucceededPercent)
        status += "      In Progress: %d (%.5f%%)\n" % (resourcesInProgress, resourcesInProgressPercent)
        status += "      Available: %d (%.5f%%)\n" % (resourcesAvailable, resourcesAvailablePercent)
        status += "      Failed: %d (%.5f%%)\n" % (resourcesFailed, resourcesFailedPercent)
        status += "      Error: %d (%.5f%%)\n" % (resourcesError, resourcesErrorPercent)
        status += "\n" + (" Status ").center(50, ':') + "\n"
    # Basic status
    else:
        status = "\n" + (" Status (%s) " % serverAddress[0]).center(50, ':') + "\n\n"
        if (clientsStatusList): 
            for clientStatus in clientsStatusList:
                clientStatus["clientid"] = "#%d" % clientStatus["clientid"]
                clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
                clientStatus["time"]["lastrequest"] = datetime.utcfromtimestamp(clientStatus["time"]["lastrequest"])
                status += "  %3s %s %s: %s since %s\n" % (
                            clientStatus["clientid"], 
                            clientStatus["threadstate"], 
                            clientStatus["address"][0], 
                            "working on %s" % clientStatus["resourceid"] if (clientStatus["resourceid"]) else "waiting for new resource", 
                            clientStatus["time"]["lastrequest"].strftime("%d/%m/%Y %H:%M:%S")
                        )
        else:
            status += "  No client connected right now.\n"
        resourcesTotal = float(serverStatus["counts"]["total"])
        resourcesProcessed = float(serverStatus["counts"]["succeeded"] + serverStatus["counts"]["failed"] + serverStatus["counts"]["error"])
        resourcesProcessedPercent = ((resourcesProcessed / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        status += "\n" + (" Status (%.5f%% completed) " % resourcesProcessedPercent).center(50, ':') + "\n"

    print status
    