# -*- coding: iso-8859-1 -*-


# The filters are sequentially applied in the same order in wich they were added to the server (through calls 
# to addFilter()), unless they were explicitly set as parallel (addFilter(parallel=True)). Only one object 
# is instanciated for each added filter, so filters must be thread safe as the same filter object is simultaneously 
# used by the multiple threads that handles different clients requests.
class BaseFilter(): 
    def __init__(self, name=""):
        self.name = self.__class__.__name__
        if (name): self.name = name
    
    def getName(self):
        return self.name
 
    # Apply must return a dictionary containing the desired filter information to be sent for the client. 
    # The value of previousFilterData will always be None if the filter is executed in parallel
    def apply(self, resourceID, responseCode, annotation, previousFilterData):
        return {"responseCode": responseCode, "annotation": annotation}
