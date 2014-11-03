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
parser.add_argument("-r", "--remove", metavar="clientID", nargs="+", help="remove clients from the server's list. Multiple client IDs can be given, separated by commas or spaces. To remove all disconnected clients, write '+' as the ID")
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
    serverStatus["shutingdown"] = "shuting down" if serverStatus["shutingdown"] else "running"
    
    # Raw status
    if (args.status == "raw"):
        status = "\n" + (" Status ").center(50, ':') + "\n\n"
        status += "  Server:\n"
        status += str("    [address, port, pid, start, state, total, succeeded, inprogress, available, failed, error]\n    ")
        status += str([serverAddress[1], serverAddress[2], serverStatus["pid"], 
                        serverStatus["time"]["start"].strftime("%d/%m/%Y %H:%M:%S"),
                        serverStatus["shutingdown"],
                        serverStatus["counts"]["total"], serverStatus["counts"]["succeeded"], 
                        serverStatus["counts"]["inprogress"], serverStatus["counts"]["available"], 
                        serverStatus["counts"]["failed"], serverStatus["counts"]["error"]])
        status += "\n\n  Clients:\n"
        if (clientsStatusList): 
            status += str("    [id, state, hostname, address, port, pid, start, lastrequest, resource, amount]\n    ")
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
                status += "  %3s %s %s (%s:%s/%s): %s since %s [%d resource%s processed in %s]\n" % (
                            clientStatus["clientid"], 
                            clientStatus["threadstate"], 
                            clientStatus["address"][0], 
                            clientStatus["address"][1], 
                            clientStatus["address"][2], 
                            clientStatus["pid"], 
                            "working on resource %s" % clientStatus["resourceid"] if (clientStatus["resourceid"]) else "waiting for resource", 
                            clientStatus["time"]["lastrequest"].strftime("%d/%m/%Y %H:%M:%S"), 
                            clientStatus["amount"], 
                            "" if (clientStatus["amount"] == 1) else "s",
                            "%02dh%02dm%02ds" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1])
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
        status += "    Server state: %s\n" % serverStatus["shutingdown"]
        status += "      Server address: %s (%s:%s/%s)\n" % (serverAddress[0], serverAddress[1], serverAddress[2], serverStatus["pid"])
        status += "      Server uptime: %s\n" % ("%02dh%02dm%02ds" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1]))
        status += "    Total number of clients: %d\n" % clientsTotal
        status += "      Connected clients: %d (%.2f%%)\n" % (connectedClients, connectedClientsPercent)
        status += "      Disconnected clients: %d (%.2f%%)\n" % (disconnectedClients, disconnectedClientsPercent)
        status += "      Clients being removed: %d (%.2f%%)\n" % (removingClients, removingClientsPercent)
        status += "    Sum of resources processed: %d\n" % sum([clientStatus["amount"] for clientStatus in clientsStatusList])
        status += "\n  " + (" Global Info ").center(46, '=') + "\n\n"
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
        status += "    Total number of resources: %d\n" % resourcesTotal
        status += "    Number of resources processed: %d (%.2f%%)\n" % (resourcesProcessed, resourcesProcessedPercent)
        status += "      Succeeded: %d (%.2f%%)\n" % (resourcesSucceeded, resourcesSucceededPercent)
        status += "      In Progress: %d (%.2f%%)\n" % (resourcesInProgress, resourcesInProgressPercent)
        status += "      Available: %d (%.2f%%)\n" % (resourcesAvailable, resourcesAvailablePercent)
        status += "      Failed: %d (%.2f%%)\n" % (resourcesFailed, resourcesFailedPercent)
        status += "      Error: %d (%.2f%%)\n" % (resourcesError, resourcesErrorPercent)
        status += "\n" + (" Status ").center(50, ':') + "\n"
    # Basic status
    else:
        status = "\n" + (" Status (%s) " % serverAddress[0]).center(50, ':') + "\n\n"
        if (clientsStatusList): 
            for clientStatus in clientsStatusList:
                clientStatus["clientid"] = "#%d" % clientStatus["clientid"]
                clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
                clientStatus["time"]["start"] = datetime.utcfromtimestamp(clientStatus["time"]["start"])
                elapsedTime = datetime.now() - clientStatus["time"]["start"]
                elapsedMinSec = divmod(elapsedTime.seconds, 60)
                elapsedHoursMin = divmod(elapsedMinSec[0], 60)
                status += "  %3s %s %s: %d resource%s processed in %s\n" % (
                            clientStatus["clientid"], 
                            clientStatus["threadstate"], 
                            clientStatus["address"][0], 
                            clientStatus["amount"], 
                            "" if (clientStatus["amount"] == 1) else "s",
                            "%02dh%02dm%02ds" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1])
                        )
        else:
            status += "  No client connected right now.\n"
        resourcesTotal = float(serverStatus["counts"]["total"])
        resourcesProcessed = float(serverStatus["counts"]["succeeded"] + serverStatus["counts"]["failed"] + serverStatus["counts"]["error"])
        resourcesProcessedPercent = ((resourcesProcessed / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        status += "\n" + (" Status (%.2f%% completed) " % resourcesProcessedPercent).center(50, ':') + "\n"

    print status
    