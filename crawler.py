# -*- coding: iso-8859-1 -*-

import os
import time
import logging


class Crawler:
    # Return a name that identifies the crawler
    def getName(self):
        return os.getpid()

    # This is the method that effectively does the collection. The ID of the resource to be collected is passed as 
    # argument to the method and the return value must be a tuple containing an integer response code and an 
    # annotation string (both are user defined). The method also receives the logging option configured in the 
    # client (true or false) and all data (if any) generated by the filters added to the server.
    def crawl(self, resourceID, loggingActive, filters):
        if (loggingActive):
            logging.info("Resource received: %s" % resourceID)
            logging.info("Filters: %s" % filters)

        print "Resource received: %s" % resourceID        
        print "Filters: %s" % filters
        
        sleepTime = 30
        print "Sleeping for %d seconds..." % sleepTime
        time.sleep(sleepTime)
        print "Awaked!"
        
        return (1, "OK")
