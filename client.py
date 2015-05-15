#!/usr/bin/python2
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
parser.add_argument("-v", "--verbose", metavar="on/off", help="enable/disable information messages on screen")
parser.add_argument("-g", "--logging", metavar="on/off", help="enable/disable logging on file")
parser.add_argument("-p", "--loggingPath", metavar="path", help="define path of logging file")
args = parser.parse_args()

# Add directory of the configuration file to sys.path before import crawler, so that the module can easily 
# be overrided by placing the modified file in a subfolder, along with the configuration file itself
configFileDir = os.path.dirname(os.path.abspath(args.configFilePath))
sys.path = [configFileDir] + sys.path
import crawler

# Load configurations
config = common.loadConfig(args.configFilePath)
if (args.verbose is not None): config["global"]["echo"]["mandatory"]["verbose"] = common.str2bool(args.verbose)
if (args.logging is not None): config["global"]["echo"]["mandatory"]["logging"] = common.str2bool(args.logging)
if (args.loggingPath is not None): config["global"]["echo"]["mandatory"]["loggingpath"] = args.loggingPath

# Connect to server
processID = os.getpid()
server = common.NetworkHandler()
server.connect(config["global"]["connection"]["address"], config["global"]["connection"]["port"])
server.send({"command": "CONNECT", "type": "client", "processid": processID})
message = server.recv()
if (message["command"] == "REFUSED"): sys.exit("ERROR: %s" % message["reason"])
else: clientID = message["clientid"]

# Configure echoing
echo = common.EchoHandler(config["client"]["echo"], "client%s[%s%s].log" % (clientID, socket.gethostname(), config["global"]["connection"]["port"]))

# Get an instance of the crawler
CrawlerClass = getattr(crawler, config["client"]["crawler"]["class"])
collector = CrawlerClass(config["client"]["crawler"])

# Execute collection
echo.out("Connected to server with ID %s." % clientID)
server.send({"command": "GET_ID"})
while (True):
    try:
        message = server.recv()
        
        if (not message): 
            echo.out("Connection to server has been abruptly closed.", "ERROR")
            break
        
        command = message["command"]
        
        if (command == "GIVE_ID"):
            resourceID = message["resourceid"]
            filters = message["filters"]
            
            try: 
                crawlerResponse = collector.crawl(resourceID, filters)
            except SystemExit: 
                echo.out("SystemExit exception while crawling resource %s. Execution aborted." % resourceID, "EXCEPTION")
                server.send({"command": "EXCEPTION", "type": "error"})
                break
            except: 
                echo.out("Exception while crawling resource %s." % resourceID, "EXCEPTION")
                server.send({"command": "EXCEPTION", "type": "fail"})
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
