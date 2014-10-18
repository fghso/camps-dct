# -*- coding: iso-8859-1 -*-

import xmltodict


# ==================== Methods ====================
def str2bool(stringToConvert):
    if stringToConvert.lower() in ("true", "t", "yes", "y", "on", "1"): return True
    if stringToConvert.lower() in ("false", "f", "no", "n", "off", "0"): return False
    raise TypeError("The value '%s' is not considered a valid boolean in this context." % stringToConvert)

    
# ==================== Classes ====================
class SystemConfiguration():
    def __init__(self, configFilePath):
        configFile = open(configFilePath, "r")
        configDict = xmltodict.parse(configFile.read())
        self.config = configDict["config"]
        self._setDefault()
        
    def _setDefault(self):
        self.config["global"]["connection"]["port"] = int(self.config["global"]["connection"]["port"])
        self.config["global"]["connection"]["bufsize"] = int(self.config["global"]["connection"]["bufsize"])
    
        if ("autofeed" not in self.config["global"]): self.config["global"]["autofeed"] = False
        else: self.config["global"]["autofeed"] = str2bool(self.config["global"]["autofeed"])
        
        if ("loopforever" not in self.config["server"]): self.config["server"]["loopforever"] = False
        else: self.config["server"]["loopforever"] = str2bool(self.config["server"]["loopforever"])
        
        if ("logging" not in self.config["server"]): self.config["server"]["logging"] = True
        else: self.config["server"]["logging"] = str2bool(self.config["server"]["logging"])
        
        if ("verbose" not in self.config["server"]): self.config["server"]["verbose"] = False
        else: self.config["server"]["verbose"] = str2bool(self.config["server"]["verbose"])
                
        if ("logging" not in self.config["client"]): self.config["client"]["logging"] = True
        else: self.config["client"]["logging"] = str2bool(self.config["client"]["logging"])
        
        if ("verbose" not in self.config["client"]): self.config["client"]["verbose"] = False
        else: self.config["client"]["verbose"] = str2bool(self.config["client"]["verbose"])
