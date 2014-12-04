# -*- coding: iso-8859-1 -*-


# The filters are sequentially applied in the same order in wich they were specified in the configuration file, 
# unless they were explicitly set as parallel. Each filter receives everything in its corresponding filter 
# section of the XML configuration file as the parameter configurationsDictionary
class BaseFilter(): 
    def __init__(self, configurationsDictionary): 
        self._extractConfig(configurationsDictionary)
        
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
    
        if ("name" in self.config): self._name = self.config["name"]
        else: self._name = self.__class__.__name__
    
    def name(self): return self._name
 
    # Apply must return a dictionary containing the desired filter information to be sent to clients. 
    # The parameter extraInfo is a reference to a dictionary and can be used to pass information among 
    # sequential filters. It is not send to clients and its value will always be None if the filter is 
    # executed in parallel
    def apply(self, resourceID, resourceInfo, extraInfo):
        return resourceInfo
        
    # Callback is called when a client is done in crawling its designated resource. Sequential filters
    # receive the parameters resourceInfo, newResources and extraInfo as references, so they can alter 
    # the values of these parameters. The server will store the final values of resourceInfo and newResources
    # as they are after all filters were called back. Parallel filters receive just a copy of the values 
    # of these three parameters. As in apply method, extraInfo can be used to pass information among sequential 
    # filters. The information received from crawler can be accessed in extraInfo["original"]. 
    def callback(self, resourceID, resourceInfo, newResources, extraInfo):
        pass
        
    def close(self): pass # Called when a connection to a client is finished
    def shutdown(self): pass # Called when server is shut down, allowing to free shared resources
