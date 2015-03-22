# -*- coding: iso-8859-1 -*-

"""Module to store filter classes.

The filters are sequentially applied in the same order in wich they were specified in the configuration file, unless they were explicitly set as parallel.

"""

import persistence


class BaseFilter(): 
    """Abstract class. All filters should inherit from it or from other class that inherits."""

    def __init__(self, configurationsDictionary): 
        """Constructor.  
        
        Each filter receives everything in its corresponding filter section of the XML configuration file as the parameter *configurationsDictionary*.
        
        """
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        """Extract and store configurations.
        
        If some configuration needs any kind of pre-processing, it is done here. Extend this method if you need to pre-process custom configuration options.
        
        """
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
        if ("name" in self.config): self.name = self.config["name"]
        else: self.name = self.__class__.__name__
    
    def setup(self): 
        """Execute per client initialization procedures.
        
        This method is called every time a connection to a new client is opened, allowing to execute initialization code on a per client basis (which differs from :meth:`__init__` that is called when the server instantiate the filter, i.e., :meth:`__init__` is called just one time for the whole period of execution of the program).
        
        """
        pass
    
    def apply(self, resourceID, resourceInfo, extraInfo): 
        """Process resource information before it is sent to a client.
        
        Args:
            * *resourceID* (user defined type): ID of the resource to be collected, sent by the server.
            * *extraInfo* (dict): Reference to a dictionary that can be used to pass information among sequential filters. It is not sent to clients and its value will always be ``None`` if the filter is executed in parallel.
            
        Returns:   
            A dictionary containing the desired filter information to be sent to clients.
        
        """
        return {}
        
    
    def callback(self, resourceID, resourceInfo, newResources, extraInfo): 
        """Process information sent by clients after a resource has been crawled.
        
        Args:
            * *resourceID* (user defined type): ID of the crawled resource.
            * *resourceInfo* (dict): Resource information dictionary sent by client. Sequential filters receive this parameter as reference, so they can alter its value, but parallel filters receive just a copy of it. The server will store the final value of *resourceInfo* as it is after all filters were called back.
            * *newResources* (list): List of new resources sent by client to be stored by the server. Sequential filters receive this parameter as reference, so they can alter its value, but parallel filters receive just a copy of it. The server will store the final value of *newResources* as it is after all filters were called back.
            * *extraInfo* (dict): Dictionary that contains information sent by client to filters. Sequential filters receive this parameter as reference, so they can alter its value, but parallel filters receive just a copy of it. As in :meth:`apply`, *extraInfo* can also be used to pass information among sequential filters (in the case of sequential filters, the original information received from crawler is stored in *extraInfo["original"]*, so it is available at any time). This information is not used by the server.
        
        """
        pass
        
    def finish(self): 
        """Execute per client finalization procedures.
        
        This method is called every time a connection to a client is closed, allowing to execute finalization code on a per client basis. It is the counterpart of :meth:`setup`.
        
        """
        pass
        
    def shutdown(self): 
        """Execute program finalization procedures (similar to a destructor).
        
        This method is called when the server is shut down, allowing to execute finalization code in a global manner. It is intended to be the counterpart of :meth:`__init__`, but differs from :meth:`__del__() <python:object.__del__>` in that it is not bounded to the live of the filter object itself, but rather to the span of execution time of the server.
        
        """
        pass
    
    
class SaveResourcesFilter(BaseFilter): 
    """Save resources sent by clients in a user specified location.
    
    This post-processing olny filter makes use of the persistence infrastructure to save resources sent by clients. The location where the resources are stored can be specified in the XML configuration file just setting up the persistence handler to be used.

    """
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        PersistenceHandlerClass = getattr(persistence, self.config["handler"]["class"])
        self.persist = PersistenceHandlerClass(self.config["handler"])
        
    def setup(self): self.persist.setup()
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        if (self.config["parallel"]): extraResources = extraInfo[self.name]
        else: extraResources = extraInfo["original"][self.name]
        self.persist.insert(extraResources)
        
    def finish(self): self.persist.finish()
    def shutdown(self): self.persist.shutdown()
        