#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import argparse
import common
import serverlib
import persistence
import filters


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", type=common.str2bool, metavar="on/off", help="enable/disable log messages on screen")
parser.add_argument("-g", "--logging", type=common.str2bool, metavar="on/off", help="enable/disable logging on file")
args = parser.parse_args()

# Load configurations
config = common.loadConfig(args.configFilePath)
if (args.verbose is not None): config["server"]["verbose"] = args.verbose
if (args.logging is not None): config["server"]["logging"] = args.logging

# Start server
server = serverlib.ThreadedTCPServer(config, persistence.MySQLPersistenceHandler)
# You can add filters to the server before start it
server.addFilter(filters.BaseFilter)
#server.addFilter(filters.BaseFilter, "MyFilterName")
#server.addFilter(filters.BaseFilter, "ParallelFilter", parallel=True)
server.start()
                