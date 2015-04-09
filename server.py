#!/usr/bin/python2
# -*- coding: iso-8859-1 -*-

import sys
import os
import argparse
import common


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", metavar="on/off", help="enable/disable information messages on screen")
parser.add_argument("-g", "--logging", metavar="on/off", help="enable/disable logging on file")
parser.add_argument("-p", "--loggingPath", metavar="path", help="define path of logging file")
args = parser.parse_args()

# Add directory of the configuration file to sys.path before import serverlib, so that persistence and filter modules
# can easily be overrided by placing the modified files in a subfolder, along with the configuration file itself
configFileDir = os.path.dirname(os.path.abspath(args.configFilePath))
sys.path = [configFileDir] + sys.path
import serverlib

# Load configurations
config = common.loadConfig(args.configFilePath)
if (args.verbose is not None): config["global"]["echo"]["mandatory"]["verbose"] = common.str2bool(args.verbose)
if (args.logging is not None): config["global"]["echo"]["mandatory"]["logging"] = common.str2bool(args.logging)
if (args.loggingPath is not None): config["global"]["echo"]["mandatory"]["loggingpath"] = args.loggingPath

# Run server
server = serverlib.ThreadedTCPServer(config)
server.run()
                