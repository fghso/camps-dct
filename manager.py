#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import json
import argparse
import common


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False, description="Send action commands to be performed by the server or retrieve status information. If none of the optional arguments are given, basic status information is shown.")
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-e", "--extended", action="store_true", help="show extended status information")
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
    if (command == "RM_RETURN"):
        removeSuccess = message["successlist"]
        removeError = message["errorlist"]
        if (not removeSuccess) and (not removeError): print "No disconnected client found to remove."
        if (len(removeSuccess) > 1): 
            print "Clients %s successfully removed." % " and ".join((", ".join(removeSuccess[:-1]), removeSuccess[-1]))
        elif (removeSuccess): 
            print "Client %s successfully removed." % removeSuccess[0]
        if (len(removeError) > 1): 
            print "ERROR: IDs %s do not exist." % " and ".join((", ".join(removeError[:-1]), removeError[-1]))
        elif (removeError): 
            print "ERROR: ID %s does not exist." % removeError[0]
    
# Shut down server
elif (args.shutdown):   
    server.send({"command": "SHUTDOWN"})
    message = server.recv()
    server.close()
    
    command = message["command"]
    if (command == "SD_OK"):
        print "Server successfully shut down."
        
# Show status
else:
    server.send({"command": "GET_STATUS"})
    message = server.recv()
    server.close()
    
    command = message["command"]
    if (command == "GIVE_STATUS"):
        print message["status"] 
