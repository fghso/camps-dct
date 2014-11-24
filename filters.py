# -*- coding: iso-8859-1 -*-


# The filters are sequentially applied in the same order in wich they were specified in the configuration file, 
# unless they were explicitly set as parallel. Each filter receives everything in its corresponding filter 
# section of the XML configuration file
class BaseFilter(): 
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
    
        if (self.config["name"]): self.name = self.config["name"]
        else: self.name = self.__class__.__name__
    
    def getName(self): return self.name
 
    # Apply must return a dictionary containing the desired filter information to be sent to the client. 
    # The value of previousFilterData will always be None if the filter is executed in parallel
    def apply(self, resourceID, resourceInfo, previousFilterData):
        return resourceInfo
        
    def close(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources
