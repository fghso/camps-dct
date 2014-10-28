#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import json
import logging
import argparse
import common


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", type=common.str2bool, metavar="on/off", help="enable/disable log messages on screen")
parser.add_argument("-g", "--logging", type=common.str2bool, metavar="on/off", help="enable/disable logging on file")
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
crawlerObject = crawler.Crawler()

# Get client ID
processID = os.getpid()
server = common.NetworkHandler()
server.connect(config["global"]["connection"]["address"], config["global"]["connection"]["port"])
server.send({"command": "GET_LOGIN", "name": crawlerObject.getName(), "processid": processID})
message = server.recv()
clientID = message["clientid"]

# Configure logging
if (config["client"]["logging"]):
    logging.basicConfig(format="%(asctime)s %(module)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename="client%s[%s%s].log" % (clientID, config["global"]["connection"]["address"], config["global"]["connection"]["port"]), filemode="w", level=logging.DEBUG)

logging.info("Connected to server with ID %s " % clientID)
if (config["client"]["verbose"]): print "Connected to server with ID %s " % clientID

# Execute collection
server.send({"command": "GET_ID"})
while (True):
    try:
        message = server.recv()
        command = message["command"]
        
        if (command == "GIVE_ID"):
            # Call crawler with resource ID and parameters received from the server
            resourceID = message["resourceid"]
            filters = message["filters"]
            crawlerResponse = crawlerObject.crawl(resourceID, filters)
            
            # Tell server that the collection of the resource has been finished. 
            # If feedback is enabled, also send the new resources to server
            if (config["global"]["feedback"]):
                server.send({"command": "DONE_ID", "resourceinfo": crawlerResponse[0], "newresources": crawlerResponse[1]})
            else: 
                server.send({"command": "DONE_ID", "resourceinfo": crawlerResponse[0]})
            
        elif (command == "DONE_RET"):
            insertErrors = message["inserterrors"]
            if (len(insertErrors) > 1): 
                logging.error("Failed to insert the new resources %s after collect resource %s." % (" and ".join((", ".join(insertErrors[:-1]), insertErrors[-1])), resourceID))
            elif (insertErrors): 
                logging.error("Failed to insert the new resource %s after collect resource %s." % (insertErrors[0], resourceID))
            server.send({"command": "GET_ID"})
                
        elif (command == "FINISH"):
            logging.info("Task done, client finished.")
            if (config["client"]["verbose"]): print "Task done, client finished."
            break
            
        elif (command == "KILL"):
            logging.info("Client removed by the server.")
            if (config["client"]["verbose"]): print "Client removed by the server."
            break
            
    except Exception as error:
        logging.exception("Exception while processing data. Execution aborted.")
        if (config["client"]["verbose"]):
            print "ERROR: %s" % str(error)
            excType, excObj, excTb = sys.exc_info()
            fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
            print (excType, fileName, excTb.tb_lineno)
        break

server.close()
