#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import json
import argparse
import common
from copy import deepcopy


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", metavar="on/off", help="enable/disable log messages on screen")
parser.add_argument("-g", "--logging", metavar="on/off", help="enable/disable logging on file")
args = parser.parse_args()

# Add directory of the configuration file to sys.path before import crawler, so that the module can easily 
# be overrided by placing the modified file in a subfolder, along with the configuration file itself
configFileDir = os.path.dirname(os.path.abspath(args.configFilePath))
sys.path = [configFileDir] + sys.path
import crawler

# Load configurations
config = common.loadConfig(args.configFilePath)
if (args.verbose is not None): config["client"]["verbose"] = args.verbose
if (args.logging is not None): config["client"]["logging"] = args.logging

# Get an instance of the crawler
collector = crawler.Crawler(deepcopy(config["client"]))

# Connect to server
processID = os.getpid()
server = common.NetworkHandler()
server.connect(config["global"]["connection"]["address"], config["global"]["connection"]["port"])
server.send({"command": "CONNECT", "type": "client", "processid": processID})
message = server.recv()
if (message["command"] == "REFUSED"): sys.exit("ERROR: %s" % message["reason"])
else: clientID = message["clientid"]

# Configure echoing
echo = common.EchoHandler(config["client"], "client%s[%s%s].log" % (clientID, socket.gethostname(), config["global"]["connection"]["port"]))

# Execute collection
echo.out("Connected to server with ID %s " % clientID)
server.send({"command": "GET_ID"})
while (True):
    try:
        message = server.recv()
        
        # Stop client execution if the connection has been interrupted
        if (not message): 
            echo.out("Connection to server has been abruptly closed.", "ERROR")
            break
        
        command = message["command"]
        
        if (command == "GIVE_ID"):
            resourceID = message["resourceid"]
            filters = message["filters"]
            
            # Try to crawl the resource
            try: 
                crawlerResponse = collector.crawl(resourceID, filters)
            # If a SystemExit exception has been raised, abort execution
            except SystemExit: 
                echo.out("SystemExit exception while crawling resource %s. Execution aborted." % resourceID, "EXCEPTION")
                server.send({"command": "EXCEPTION", "type": "error"})
                break
            # If another type of exception has been raised, report fail
            except: 
                echo.out("Exception while crawling resource %s." % resourceID, "EXCEPTION")
                server.send({"command": "EXCEPTION", "type": "fail"})
            # If everything is ok, tell server that the collection of the resource has been finished. 
            # If feedback is enabled, also send the new resources to server
            else:
                resourceInfo = crawlerResponse[0]
                extraInfo = crawlerResponse[1]
                newResources = None
                if (config["global"]["feedback"]): newResources = crawlerResponse[2]
                server.send({"command": "DONE_ID", "resourceinfo": resourceInfo, "extrainfo": extraInfo, "newresources": newResources})
            
        elif (command == "DONE_RET") or (command == "EXCEPTION_RET"):
            server.send({"command": "GET_ID"})
            
        elif (command == "FINISH"):
            reason = message["reason"]
            if (reason == "task done"): echo.out("Task done, client finished.")
            elif (reason == "shut down"): echo.out("Server shuting down, client finished.")
            else: echo.out("Client manually removed.")
            break
            
    except:
        echo.out("Exception while processing data. Execution aborted.", "EXCEPTION")
        break

server.close()
