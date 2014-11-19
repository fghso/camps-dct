# -*- coding: iso-8859-1 -*-

import socket
import time
import logging


class Crawler:
    # This is the method that effectively does the collection. The method receives as arguments the ID of the resource to 
    # be collected and all data (if any) generated by the filters added to server. The return value must be a tuple  
    # containing a resource information dictionary at the first position. This information is user defined and must be 
    # understood by the persistence handler used. If feedback option is enabled, the resources to be stored can be given 
    # in a list as the second element of the tuple. Each new resource is described by a tuple, where the first element is 
    # the resource ID and the second element is a dictionary containing resource information (also in a format understood by 
    # the persistence handler used)
    def crawl(self, resourceID, filters):
        logging.info("Resource received: %s" % resourceID)
        logging.info("Filters: %s" % filters)
        
        print "Resource received: %s" % resourceID        
        print "Filters: %s" % filters
        
        sleepTime = 10
        print "Sleeping for %d seconds..." % sleepTime
        time.sleep(sleepTime)
        print "Awaked!\n"
        
        newResources = []
        newResources.append((resourceID + 1, {"crawler_name": socket.gethostname(), "response_code": 3}))
        newResources.append((resourceID + 2, {"crawler_name": socket.gethostname(), "response_code": 3}))
        
        return ({"crawler_name": socket.gethostname(), "response_code": 4}, newResources)
            