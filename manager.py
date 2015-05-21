#!/usr/bin/python2
# -*- coding: iso-8859-1 -*-

import sys
import json
import argparse
import common


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False, description="Send action commands to be performed by the server or retrieve status information. If none of the optional arguments are given, show basic status information.")
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-r", "--remove", metavar="client ID or client hostname", nargs="+", help="remove clients from the server's list. Multiple client IDs or hostnames can be given, separated by spaces. It is also possible to enter an ID range in the form 'min:max', where min and max are IDs. To remove all disconnected clients, use the keyword 'disconnected'. To remove all clients at once, use the keyword 'all'")
parser.add_argument("--reset", choices=["succeeded", "inprogress", "failed", "error"], help="make available the resources with the specified status")
parser.add_argument("--shutdown", action="store_true", help="remove all clients from the server's list and shut down server")
parser.add_argument("-s", "--status", choices=["raw", "basic", "extended"], help="show status information")
args = parser.parse_args()

# Load configurations
config = common.loadConfig(args.configFilePath)

# Connect to server
try:
    server = common.NetworkHandler()
    server.connect(config["global"]["connection"]["address"], config["global"]["connection"]["port"])
except:
    sys.exit("ERROR: It was not possible to connect to server at %s:%s." % (config["global"]["connection"]["address"], config["global"]["connection"]["port"]))
    
server.send({"command": "CONNECT", "type": "manager"})
message = server.recv()
if (message["command"] == "REFUSED"): sys.exit("ERROR: %s" % message["reason"])

# Remove client
if (args.remove):
    clientIDs = set()
    clientNames = set()

    for entry in args.remove:
        if entry.isdigit(): 
            clientIDs.add(int(entry))
        else:
            minMax = entry.split(":")
            if (len(minMax) > 1): clientIDs.update(range(int(minMax[0]), int(minMax[1]) + 1))
            else: clientNames.add(entry)
            
    server.send({"command": "RM_CLIENTS", "clientids": clientIDs, "clientnames": clientNames})
    message = server.recv()
    server.close()
    
    removeSuccess = message["successlist"]
    removeError = message["errorlist"]
    if (not removeSuccess) and (not removeError): print "No client in the list found to remove."
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
    
    # Raw status
    if (args.status == "raw"):
        status = "\n" + (" Status ").center(50, ':') + "\n\n"
        status += "  Server:\n"
        status += str("    [address, port, pid, state, start, current, total, succeeded, inprogress, available, failed, error]\n    ")
        status += str([serverAddress[1], serverAddress[2], serverStatus["pid"], 
                        serverStatus["state"],
                        serverStatus["time"]["start"].strftime("%d/%m/%Y %H:%M:%S"),
                        serverStatus["time"]["current"].strftime("%d/%m/%Y %H:%M:%S"),
                        serverStatus["counts"]["total"], serverStatus["counts"]["succeeded"], 
                        serverStatus["counts"]["inprogress"], serverStatus["counts"]["available"], 
                        serverStatus["counts"]["failed"], serverStatus["counts"]["error"]])
        status += "\n\n  Clients:\n"
        if (clientsStatusList): 
            status += str("    [id, state, hostname, address, port, pid, start, lastrequest, agrserver, agrclient, agrcrawler, timingmeasures, crawlingmeasures, resource, amount]\n    ")
        else: 
            status += "    No client connected right now.\n"
        for clientStatus in clientsStatusList:
            clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
            status += str([clientStatus["clientid"], clientStatus["threadstate"], str(clientStatus["address"][0]), 
                        str(clientStatus["address"][1]), clientStatus["address"][2], clientStatus["pid"],
                        clientStatus["time"]["start"].strftime("%d/%m/%Y %H:%M:%S"), 
                        clientStatus["time"]["lastrequest"].strftime("%d/%m/%Y %H:%M:%S") if (clientStatus["time"]["lastrequest"] is not None) else "-",
                        "%.2f" % clientStatus["time"]["agrserver"], 
                        "%.2f" % clientStatus["time"]["agrclient"],
                        "%.2f" % clientStatus["time"]["agrcrawler"], 
                        clientStatus["time"]["timingmeasures"], 
                        clientStatus["time"]["crawlingmeasures"],
                        clientStatus["resourceid"] if (clientStatus["resourceid"]) else "waiting", 
                        clientStatus["amount"]])
            status += "\n    "
        status += "\n" + (" Status ").center(50, ':') + "\n"
        
    # Extended status
    elif (args.status == "extended"):
        status = "\n" + (" Status ".center(50, ':')) + "\n\n"
        clientsElapsedTimes = []
        if (clientsStatusList): 
            for clientStatus in clientsStatusList:
                clientStatus["clientid"] = "#%d" % clientStatus["clientid"]
                clientStatus["threadstate"] = " " if (clientStatus["threadstate"] == 0) else ("-" if (clientStatus["threadstate"] == -1) else "+")
                elapsedTime = (serverStatus["time"]["current"] - clientStatus["time"]["start"]).total_seconds()
                elapsedMinSec = divmod(elapsedTime, 60)
                elapsedHoursMin = divmod(elapsedMinSec[0], 60)
                clientsElapsedTimes.append(elapsedTime)
                status += "  %3s %s %s (%s:%s/%s): %s since %s [%d processed in %s]\n" % (
                            clientStatus["clientid"], 
                            clientStatus["threadstate"], 
                            clientStatus["address"][0], 
                            clientStatus["address"][1], 
                            clientStatus["address"][2], 
                            clientStatus["pid"], 
                            "working on %s" % clientStatus["resourceid"] if (clientStatus["resourceid"]) else "waiting for new resource", 
                            clientStatus["time"]["lastrequest"].strftime("%d/%m/%Y %H:%M:%S") if (clientStatus["time"]["lastrequest"] is not None) else "-", 
                            clientStatus["amount"], 
                            #"" if (clientStatus["amount"] == 1) else "s",
                            "%02d:%02d:%02d" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1])
                        )
        else:
            status += "  No client connected right now.\n"

        serverElapsedTime = (serverStatus["time"]["current"] - serverStatus["time"]["start"]).total_seconds()
        serverElapsedMinSec = divmod(serverElapsedTime, 60)
        serverElapsedHoursMin = divmod(serverElapsedMinSec[0], 60)
        
        sumClientsElapsedTimes = sum(clientsElapsedTimes)
        sumAgrServerTime = sum([clientStatus["time"]["agrserver"] for clientStatus in clientsStatusList])
        sumAgrClientTime = sum([clientStatus["time"]["agrclient"] for clientStatus in clientsStatusList])
        sumAgrCrawlerTime = sum([clientStatus["time"]["agrcrawler"] for clientStatus in clientsStatusList])
        #sumAgrTotalTime = sumAgrServerTime + sumAgrClientTime
        fractionServerTime = sumAgrServerTime / sumClientsElapsedTimes if (sumClientsElapsedTimes > 0) else 0.0
        #fractionServerTime = sumAgrServerTime / sumAgrTotalTime if (sumAgrTotalTime > 0) else 0.0
        proportionalServerTime = fractionServerTime * serverElapsedTime
        proportionalServerMinSec = divmod(proportionalServerTime, 60)
        proportionalServerHoursMin = divmod(proportionalServerMinSec[0], 60)
        proportionalServerTimePercent = fractionServerTime * 100
        fractionClientTime = sumAgrClientTime / sumClientsElapsedTimes if (sumClientsElapsedTimes > 0) else 0.0
        #fractionClientTime = sumAgrClientTime / sumAgrTotalTime if (sumAgrTotalTime > 0) else 0.0
        proportionalClientTime = fractionClientTime * serverElapsedTime
        proportionalClientMinSec = divmod(proportionalClientTime, 60)
        proportionalClientHoursMin = divmod(proportionalClientMinSec[0], 60)
        proportionalClientTimePercent = fractionClientTime * 100
        fractionCrawlerTime = sumAgrCrawlerTime / sumClientsElapsedTimes if (sumClientsElapsedTimes > 0) else 0.0
        #fractionCrawlerTime = sumAgrCrawlerTime / sumAgrTotalTime if (sumAgrTotalTime > 0) else 0.0
        proportionalCrawlerTime = fractionCrawlerTime * serverElapsedTime
        proportionalCrawlerMinSec = divmod(proportionalCrawlerTime, 60)
        proportionalCrawlerHoursMin = divmod(proportionalCrawlerMinSec[0], 60)
        proportionalCrawlerTimePercent = fractionCrawlerTime * 100
        performanceIndicator = "good"
        if (proportionalServerTimePercent >= 25): performanceIndicator = "moderate"
        if (proportionalServerTimePercent >= 50): performanceIndicator = "bad"
        if (proportionalServerTimePercent >= 75): performanceIndicator = "ugly"
        
        clientsTotal = float(len(clientsStatusList))
        connectedClients = float(len([client for client in clientsStatusList if client["threadstate"] == " "]))
        disconnectedClients = float(len([client for client in clientsStatusList if client["threadstate"] == "+"]))
        removingClients = float(len([client for client in clientsStatusList if client["threadstate"] == "-"]))
        workingClients = float(len([client for client in clientsStatusList if (client["threadstate"] == " " and client["resourceid"])]))
        waitingClients = float(len([client for client in clientsStatusList if (client["threadstate"] == " " and not client["resourceid"])]))
        connectedClientsPercent = ((connectedClients / clientsTotal) * 100) if (clientsTotal > 0) else 0.0
        disconnectedClientsPercent = ((disconnectedClients / clientsTotal) * 100) if (clientsTotal > 0) else 0.0
        removingClientsPercent = ((removingClients / clientsTotal) * 100) if (clientsTotal > 0) else 0.0
        workingClientsPercent = ((workingClients / connectedClients) * 100) if (connectedClients > 0) else 0.0
        waitingClientsPercent = ((waitingClients / connectedClients) * 100) if (connectedClients > 0) else 0.0
        
        sumTimingMeasures = sum([clientStatus["time"]["timingmeasures"] for clientStatus in clientsStatusList])
        sumCrawlingMeasures = sum([clientStatus["time"]["crawlingmeasures"] for clientStatus in clientsStatusList])
        avgServerTime = sumAgrServerTime / sumTimingMeasures if (sumTimingMeasures > 0) else 0.0
        avgServerMinSec = divmod(avgServerTime / clientsTotal, 60) if (clientsTotal > 0) else (0,0)
        avgServerHoursMin = divmod(avgServerMinSec[0], 60)
        avgClientTime = sumAgrClientTime / sumTimingMeasures if (sumTimingMeasures > 0) else 0.0
        avgClientMinSec = divmod(avgClientTime / clientsTotal, 60) if (clientsTotal > 0) else (0,0)
        avgClientHoursMin = divmod(avgClientMinSec[0], 60)
        avgCrawlerTime = sumAgrCrawlerTime / sumCrawlingMeasures if (sumCrawlingMeasures > 0) else 0.0
        avgCrawlerMinSec = divmod(avgCrawlerTime / clientsTotal, 60) if (clientsTotal > 0) else (0,0)
        avgCrawlerHoursMin = divmod(avgCrawlerMinSec[0], 60)
        
        # serverElapsedTime is not used here to calculate the average number of resources per 
        # second to avoid accouting server idle time. Thus clientElapsedTime is used instead
        numResourcesProcessed = float(sum([clientStatus["amount"] for clientStatus in clientsStatusList]))
        avgResourcesPerclient = numResourcesProcessed / clientsTotal if (clientsTotal > 0) else 0.0
        clientsResourcesPerSec = [float(clientStatus["amount"]) / clientElapsedTime if (clientElapsedTime > 0) else 0.0 for (clientStatus, clientElapsedTime) in zip(clientsStatusList, clientsElapsedTimes)]
        avgResourcesPerSec = sum(clientsResourcesPerSec)
        
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
        
        # Another way to infer estimatedTimeToFinish is using the average time for a request/response round trip. 
        # To collect a resource, the client has to make at least two requests: one to get a resource ID to crawl 
        # and another one to signal that the crawling process has been completed (either if it succeeded or failed). 
        # The average round trip time is the sum of the average server processing time per request plus the average 
        # client processing time per response. So, the final code is: 
        # estimatedTimeToFinish = (avgServerMinSec[1] + avgClientMinSec[1]) * 2 * (resourcesAvailable + resourcesInProgress)
        estimatedTimeToFinish = (1.0 / avgResourcesPerSec) * (resourcesAvailable + resourcesInProgress) if (avgResourcesPerSec > 0) else 0.0
        estimatedMinSec = divmod(estimatedTimeToFinish, 60)
        estimatedHoursMin = divmod(estimatedMinSec[0], 60)
        
        status += "\n  " + (" Session Info ").center(46, '=') + "\n\n"
        status += "    Server state: %s\n" % serverStatus["state"]
        status += "      Server address: %s (%s:%s/%s)\n" % (serverAddress[0], serverAddress[1], serverAddress[2], serverStatus["pid"])
        status += "      Server uptime: %s\n" % ("%02d:%02d:%02d" % (serverElapsedHoursMin[0],  serverElapsedHoursMin[1], serverElapsedMinSec[1]))
        status += "      Estimated time to finish: %s\n" % ("%02d:%02d:%02d" % (estimatedHoursMin[0],  estimatedHoursMin[1], estimatedMinSec[1]))
        
        status += "    Server performance: %s\n" % performanceIndicator
        status += "      Average proportional server time: %s (%.2f%%)\n" % ("%02d:%02d:%08.5f" % (proportionalServerHoursMin[0],  proportionalServerHoursMin[1], proportionalServerMinSec[1]), proportionalServerTimePercent)
        status += "      Average proportional client time: %s (%.2f%%)\n" % ("%02d:%02d:%08.5f" % (proportionalClientHoursMin[0], proportionalClientHoursMin[1], proportionalClientMinSec[1]), proportionalClientTimePercent)
        status += "      Average proportional crawler time: %s (%.2f%%)\n" % ("%02d:%02d:%08.5f" % (proportionalCrawlerHoursMin[0], proportionalCrawlerHoursMin[1], proportionalCrawlerMinSec[1]), proportionalCrawlerTimePercent)
        status += "      Average server time per request: %s\n" % ("%02d:%02d:%08.5f" % (avgServerHoursMin[0],  avgServerHoursMin[1], avgServerMinSec[1]))
        status += "      Average client time per response: %s\n" % ("%02d:%02d:%08.5f" % (avgClientHoursMin[0],  avgClientHoursMin[1], avgClientMinSec[1]))
        status += "      Average crawler time per client: %s\n" % ("%02d:%02d:%08.5f" % (avgCrawlerHoursMin[0],  avgCrawlerHoursMin[1], avgCrawlerMinSec[1]))
        
        status += "    Total number of clients: %d\n" % clientsTotal
        status += "      Connected clients: %d (%.2f%%)\n" % (connectedClients, connectedClientsPercent)
        status += "      Disconnected clients: %d (%.2f%%)\n" % (disconnectedClients, disconnectedClientsPercent)
        status += "      Clients being removed: %d (%.2f%%)\n" % (removingClients, removingClientsPercent)
        status += "      Connected clients working: %d (%.2f%%)\n" % (workingClients, workingClientsPercent)
        status += "      Connected clients waiting: %d (%.2f%%)\n" % (waitingClients, waitingClientsPercent)
        
        status += "    Number of resources processed: %d\n" % numResourcesProcessed
        status += "      Average resources processed per client: %.2f\n" % avgResourcesPerclient
        status += "      Average resources processed per time unit: %.2f/h, %.2f/m, %.2f/s\n" % (avgResourcesPerSec * 3600, avgResourcesPerSec * 60, avgResourcesPerSec)
        
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
                status += "  %3s %s %s: %s since %s\n" % (
                            clientStatus["clientid"], 
                            clientStatus["threadstate"], 
                            clientStatus["address"][0], 
                            "working on %s" % clientStatus["resourceid"] if (clientStatus["resourceid"]) else "waiting for new resource", 
                            clientStatus["time"]["lastrequest"].strftime("%d/%m/%Y %H:%M:%S") if (clientStatus["time"]["lastrequest"] is not None) else "-"
                        )
        else:
            status += "  No client connected right now.\n"
        resourcesTotal = float(serverStatus["counts"]["total"])
        resourcesProcessed = float(serverStatus["counts"]["succeeded"] + serverStatus["counts"]["failed"] + serverStatus["counts"]["error"])
        resourcesProcessedPercent = ((resourcesProcessed / resourcesTotal) * 100) if (resourcesTotal > 0) else 0.0
        status += "\n" + (" Status (%.5f%% completed) " % resourcesProcessedPercent).center(50, ':') + "\n"

    print status
    