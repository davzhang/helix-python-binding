# package org.apache.helix.util
#from org.apache.helix.util import *
#from java.util import Arrays
#from java.util import HashMap
#from java.util import Map
#from java.util.regex import Matcher
#from java.util.regex import Pattern
#from org.apache.log4j import Logger

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.UserExceptions import IllegalArgumentException
import re

class StringTemplate:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
#    LOG = Logger.getLogger(StringTemplate.class)
    LOG = get_logger(__name__)


    """
    Java modifiers:
         static
    Type:
        Pattern
    """

    def __init__(self):
        self.templateMap = {}
        self.pattern = re.compile("({.+?})")

    def addEntry(self, type, numKeys, template):
        """
        Returns void
        Parameters:
            type: EnumnumKeys: inttemplate: String


        """
        if not self.templateMap.__contains__(type):
            self.templateMap.__setitem__(type, {})
#            self.templateMap.__setitem__(type, HashMap<Integer, String>())

#        import pdb; pdb.set_trace()
        # dzhang:may need to pass in name from the caller
        self.LOG.trace("Add template for type: " + str(type) + ", arguments: " + str(numKeys) + ", template: " + template)
        self.templateMap.get(type).__setitem__(numKeys, template)


    def instantiate(self, type, keys):
        """
        Returns String
        Parameters:
            type: Enumkeys: String


        """
        if keys == None: 
            keys = []
#            keys = new String[] {}

        # String
        template = None
        if self.templateMap.__contains__(type):
            template = self.templateMap.get(type).get(len(keys))

        # String
        result = None
        if template != None: 
            result = template
            # Matcher
            matches = self.pattern.findall(template)
            # int
            count = 0
#            while (matcher.find():
            for var in matches:
                # String
#                var = matcher.group()
                result = result.replace(var, keys[count])
                count +=1


        if result == None or result.find('{') > -1 or result.find('}') > -1:
            # String
            errMsg = "Unable to instantiate template: " + template + " using keys: " + keys
#            errMsg = "Unable to instantiate template: " + template + " using keys: " + Arrays.toString(keys)
            self.LOG.error(errMsg)
            raise IllegalArgumentException(errMsg)


        return result

