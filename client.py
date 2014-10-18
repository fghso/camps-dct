#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import json
import logging
import argparse
import common
import crawler


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", type=common.str2bool, metavar="on/off", help="enable/disable log messages on screen")
parser.add_argument("-g", "--logging", type=common.str2bool, metavar="on/off", help="enable/disable logging on file")
args = parser.parse_args()

# Load configurations
config = common.SystemConfiguration(args.configFilePath).config
if (args.verbose is not None): config["client"]["verbose"] = args.verbose
if (args.logging is not None): config["client"]["logging"] = args.logging

# Get an instance of the crawler
crawlerObj = crawler.Crawler()

# Get client ID
processID = os.getpid()
server = socket.socket()
server.connect((config["global"]["connection"]["address"], config["global"]["connection"]["port"]))
server.send(json.dumps({"command": "GET_LOGIN", "name": crawlerObj.getName(), "processid": processID}))
response = server.recv(config["global"]["connection"]["bufsize"])
message = json.loads(response)
clientID = message["clientid"]

# Configure logging
if (config["client"]["logging"]):
    logging.basicConfig(format="%(asctime)s %(module)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename="client%s[%s%s].log" % (clientID, config["global"]["connection"]["address"], config["global"]["connection"]["port"]), filemode="w", level=logging.DEBUG)
    logging.info("Connected to server with ID %s " % clientID)
if (config["client"]["verbose"]): print "Connected to server with ID %s " % clientID

# Execute collection
server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))
while (True):
    try:
        response = server.recv(config["global"]["connection"]["bufsize"])

        # Extract command
        message = json.loads(response)
        command = message["command"]
        
        if (command == "GIVE_ID"):
            # Call crawler with resource ID and parameters received from the server
            resourceID = message["resourceid"]
            filters = message["filters"]
            crawlerResponse = crawlerObj.crawl(resourceID, config["client"]["logging"], filters)
            
            # Tell server that the collection of the resource has been finished
            server.send(json.dumps({"command": "DONE_ID", "clientid": clientID, "resourceid": resourceID, "responsecode": crawlerResponse[0], "annotation": crawlerResponse[1]}))

        elif (command == "DID_OK"):
            # Get a new resource ID
            server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))
            
        elif (command == "FINISH"):
            if (config["client"]["logging"]): logging.info("Task done, client finished.")
            if (config["client"]["verbose"]): print "Task done, client finished."
            break
            
        elif (command == "KILL"):
            if (config["client"]["logging"]): logging.info("Client removed by the server.")
            if (config["client"]["verbose"]): print "Client removed by the server."
            break
            
    except Exception as error:
        if (config["client"]["logging"]): logging.exception("Exception while processing data. Execution aborted.")
        if (config["client"]["verbose"]):
            print "ERROR: %s" % str(error)
            excType, excObj, excTb = sys.exc_info()
            fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
            print (excType, fileName, excTb.tb_lineno)
        break

server.shutdown(socket.SHUT_RDWR)
server.close()
