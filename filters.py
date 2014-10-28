# -*- coding: iso-8859-1 -*-


# The filters are sequentially applied in the same order in wich they were specified in the 
# configuration file, unless they were explicitly set as parallel in the configuration 
class BaseFilter(): 
    def __init__(self, name):
        if (name): self.name = name
        else: self.name = self.__class__.__name__
    
    def getName(self):
        return self.name
 
    # Apply must return a dictionary containing the desired filter information to be sent to the client. 
    # The value of previousFilterData will always be None if the filter is executed in parallel
    def apply(self, resourceID, resourceInfo, previousFilterData):
        return resourceInfo
