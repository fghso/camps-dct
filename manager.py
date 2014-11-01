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
    
    command = message["command"]
    if (message["fail"]):
        print "ERROR: %s" % message["reason"]
    else:
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
    
# Shut down server
elif (args.shutdown):   
    server.send({"command": "SHUTDOWN"})
    message = server.recv()
    server.close()
    
    command = message["command"]
    if (message["fail"]): print "ERROR: %s" % message["reason"]
    else: print "Server successfully shut down."
        
# Show status
else:
    server.send({"command": "GET_STATUS"})
    message = server.recv()
    serverAddress = server.getaddress()
    server.close()
    
    command = message["command"]
    clientsStatusList = message["clients"]
    serverStatus = message["server"]
    serverStatus["time"]["start"] = datetime.utcfromtimestamp(serverStatus["time"]["start"])
    
    # Raw status
    if (args.status == "raw"):
        status = "\nServer:\n"
        status += str("  [address, port, pid, start, total, succeeded, inprogress, available, failed, error]\n  ")
        status += str([serverAddress[1], serverAddress[2], serverStatus["pid"], 
                        serverStatus["time"]["start"].strftime("%d/%m/%Y %H:%M:%S"), 
                        serverStatus["counts"]["total"], serverStatus["counts"]["succeeded"], 
                        serverStatus["counts"]["inprogress"], serverStatus["counts"]["available"], 
                        serverStatus["counts"]["failed"], serverStatus["counts"]["error"]])
        status += "\n\nClients:\n"
        if (clientsStatusList): 
            status += str("  [id, state, hostname, address, port, pid, start, update, resource, amount]\n  ")
        else: 
            status += "  No client connected right now.\n"
        for clientStatus in clientsStatusList:
            clientStatus["time"]["start"] = datetime.utcfromtimestamp(clientStatus["time"]["start"])
            clientStatus["time"]["lastupdate"] = datetime.utcfromtimestamp(clientStatus["time"]["lastupdate"])
            status += str([clientStatus["clientid"], clientStatus["threadstate"], str(clientStatus["address"][0]), 
                        str(clientStatus["address"][1]), clientStatus["address"][2], clientStatus["pid"],
                        clientStatus["time"]["start"].strftime("%d/%m/%Y %H:%M:%S"), 
                        clientStatus["time"]["lastupdate"].strftime("%d/%m/%Y %H:%M:%S"),                            
                        clientStatus["resourceid"], clientStatus["amount"]])
            status += "\n  "
    # Extended status
    elif (args.status == "extended"):
        status = "\n" + (" Status (%s|%s:%s/%s) " % (serverAddress[0], serverAddress[1], serverAddress[2], serverStatus["pid"])).center(50, ':') + "\n\n"
        if (clientsStatusList): 
            for clientStatus in clientsStatusList:
                clientStatus["clientid"] = "#%d" % clientStatus["clientid"]
                clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
                clientStatus["time"]["start"] = datetime.utcfromtimestamp(clientStatus["time"]["start"])
                clientStatus["time"]["lastupdate"] = datetime.utcfromtimestamp(clientStatus["time"]["lastupdate"])
                elapsedTime = datetime.now() - clientStatus["time"]["start"]
                elapsedMinSec = divmod(elapsedTime.seconds, 60)
                elapsedHoursMin = divmod(elapsedMinSec[0], 60)
                status += "  %3s %s %s (%s:%s/%s): %s since %s [%d resource%s received in %s]\n" % (
                            clientStatus["clientid"], 
                            clientStatus["threadstate"], 
                            clientStatus["address"][0], 
                            clientStatus["address"][1], 
                            clientStatus["address"][2], 
                            clientStatus["pid"], 
                            clientStatus["resourceid"], 
                            clientStatus["time"]["lastupdate"].strftime("%d/%m/%Y %H:%M:%S"), 
                            clientStatus["amount"], 
                            "" if (clientStatus["amount"] == 1) else "s",
                            "%02dh%02dm%02ds" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1])
                        )
        else:
            status += "  No client connected right now.\n"
        resourcesTotal = float(serverStatus["counts"]["total"])
        resourcesCollected = float(serverStatus["counts"]["succeeded"] + serverStatus["counts"]["failed"])
        collectedResourcesPercent = (resourcesCollected / resourcesTotal) * 100
        status += "\n" + (" Status (%.2f%% completed) " % collectedResourcesPercent).center(50, ':') + "\n"
    # Basic status
    else:
        status = "\n" + (" Status (%s) " % serverAddress[0]).center(50, ':') + "\n\n"
        if (clientsStatusList): 
            for clientStatus in clientsStatusList:
                clientStatus["clientid"] = "#%d" % clientStatus["clientid"]
                clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
                clientStatus["time"]["start"] = datetime.utcfromtimestamp(clientStatus["time"]["start"])
                clientStatus["time"]["lastupdate"] = datetime.utcfromtimestamp(clientStatus["time"]["lastupdate"])
                elapsedTime = datetime.now() - clientStatus["time"]["start"]
                elapsedMinSec = divmod(elapsedTime.seconds, 60)
                elapsedHoursMin = divmod(elapsedMinSec[0], 60)
                status += "  %3s %s %s: %d resource%s received in %s\n" % (
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
        resourcesCollected = float(serverStatus["counts"]["succeeded"] + serverStatus["counts"]["failed"] + serverStatus["counts"]["error"])
        collectedResourcesPercent = (resourcesCollected / resourcesTotal) * 100
        status += "\n" + (" Status (%.2f%% completed) " % (collectedResourcesPercent)).center(50, ':') + "\n"

    print status
    