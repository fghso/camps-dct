# -*- coding: iso-8859-1 -*-

import persistence


# The filters are sequentially applied in the same order in wich they were specified in the configuration file, 
# unless they were explicitly set as parallel. Each filter receives everything in its corresponding filter 
# section of the XML configuration file as the parameter configurationsDictionary
class BaseFilter(): 
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        if ("name" in self.config): self.name = self.config["name"]
        else: self.name = self.__class__.__name__
    
    def setup(self): pass # Called when a connection to a client is opened
 
    # Apply must return a dictionary containing the desired filter information to be sent to clients. 
    # The parameter extraInfo is a reference to a dictionary and can be used to pass information among 
    # sequential filters. It is not send to clients and its value will always be None if the filter is 
    # executed in parallel
    def apply(self, resourceID, resourceInfo, extraInfo): return {}
        
    # Callback is called when a client is done in crawling its designated resource. Sequential filters
    # receive the parameters resourceInfo, newResources and extraInfo as references, so they can alter 
    # the values of these parameters. The server will store the final values of resourceInfo and newResources
    # as they are after all filters were called back. Parallel filters receive just a copy of the values 
    # of these three parameters as they came from crawler. As in apply method, extraInfo can be used to pass 
    # information among sequential filters (in the case of sequential filters, the original information received 
    # from crawler is stored in extraInfo["original"], so it is available at any time). 
    def callback(self, resourceID, resourceInfo, newResources, extraInfo): pass
        
    def finish(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources
    
    
class SaveResourcesFilter(BaseFilter): 
    def __init__(self, configurationsDictionary): 
        BaseFilter.__init__(self, configurationsDictionary)
        PersistenceHandlerClass = getattr(persistence, self.config["handler"]["class"])
        self.persist = PersistenceHandlerClass(self.config["handler"])
        
    def setup(self): self.persist.setup()
        
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        if (self.config["parallel"]): newResources = extraInfo[self.name]
        else: newResources = extraInfo["original"][self.name]
        self.persist.insert(newResources)
        
    def finish(self): self.persist.finish()
    def shutdown(self): self.persist.shutdown()
        