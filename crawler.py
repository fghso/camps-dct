# -*- coding: iso-8859-1 -*-

"""Module to store crawler classes.

More than one class can be written here, but only one (that specified in the configuration file) will be used by the client to instantiate a crawler object whose :meth:`crawl() <BaseCrawler.crawl>` method will by called to do the collection of the resource received. 

"""

import socket
import time
import common


class BaseCrawler:
    """Abstract class. All crawlers should inherit from it or from other class that inherits."""

    def __init__(self, configurationsDictionary):
        """Constructor.  
        
        Upon initialization the crawler object receives everything in the crawler section of the XML configuration file as the parameter *configurationsDictionary*. 
        
        """
        self._extractConfig(configurationsDictionary)
        self.echo = common.EchoHandler(self.config["echo"])
       
    def _extractConfig(self, configurationsDictionary):
        """Extract and store configurations.
        
        If some configuration needs any kind of pre-processing, it is done here. Extend this method if you need to pre-process custom configuration options.
        
        """
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
    
    def crawl(self, resourceID, filters):
        """Collect the resource.
        
        Must be overriden.
        
        Args:
            * *resourceID* (user defined type): ID of the resource to be collected, sent by the server.
            * *filters* (list): All data (if any) generated by the filters added to server. Sequential filters data come first, in the same order that the filters were specified in the configuration file. Parallel filters data come next, in undetermined order.
            
        Returns:   
            A tuple in the format (*resourceInfo*, *extraInfo*, *newResources*). Any element of the tuple can be ``None``, depending on what the user desires.

            * *resourceInfo* (dict): Resource information dictionary, used to update resource information at the server side. This information is user defined and must be understood by the persistence handler used. 
            * *extraInfo* (dict): Aditional information. This information is just passed to all filters via :meth:`callback() <filters.BaseFilter.callback>` method and is not used by the server itself. 
            * *newResources* (list): Resources to be stored by the server when the feedback option is enabled. Each new resource is described by a tuple in the format (*resourceID*, *resourceInfo*), where the first element is the resource ID (whose type is defined by the user) and the second element is a dictionary containing resource information (in a format understood by the persistence handler used).
                
        """
        return (None, None, None)
        

class DemoCrawler(BaseCrawler):
    """Example crawler, just for demonstration."""

    def crawl(self, resourceID, filters):
        self.echo.out("Resource received: %s" % resourceID)
        
        sleepTime = 30
        self.echo.out("Sleeping for %d seconds..." % sleepTime)
        time.sleep(sleepTime)
        self.echo.out("Awaked!\n")
        
        newResources = []
        newResources.append((resourceID + 1, {"crawler_name": "c1", "response_code": 3}))
        newResources.append((resourceID + 2, {"crawler_name": "c2", "response_code": 4}))
        extraInfo = {"savecsv": newResources[:1], "savejson": newResources[1:]}
        
        return ({"crawler_name": socket.gethostname(), "response_code": 5}, extraInfo, newResources)
        