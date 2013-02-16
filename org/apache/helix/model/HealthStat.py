# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import Date
#from java.util import HashMap
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.alerts.ExpressionParser import ExpressionParser
from org.apache.helix.alerts.StatsHolder import StatsHolder
from org.apache.helix.model.Message import Attributes

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum, ternary
import time
HealthStatProperty = enum('FIELDS')

class HealthStat(HelixProperty):


    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    _logger = get_logger(__name__)

    """

    Parameters:
        String id
    """
    def __init__(self, id):
        super(id)


    """

    Parameters:
        ZNRecord record
    """
    def __init__(self, record):
        super(record)
        if self.getCreateTimeStamp() == 0:
            self._record.setSimpleField(Attributes.CREATE_TIMESTAMP.toString(), "" + time.time())



    def getLastModifiedTimeStamp(self):
        """
        Returns long


        """
        return self._record.getModifiedTime()


    def getCreateTimeStamp(self):
        """
        Returns long


        """
        if self._record.getSimpleField(Attributes.CREATE_TIMESTAMP.toString()) == None:
            return 0

        try:
            return long(self._record.getSimpleField(Attributes.toString(Attributes.CREATE_TIMESTAMP)))
        except ValueError, e:
            return 0



    def getTestField(self):
        """
        Returns String


        """
        return self._record.getSimpleField("requestCountStat")


    def setHealthFields(self, healthFields):
        """
        Returns void
        Parameters:
            healthFields: Map<String, Map<String, String>>


        """
        self._record.setMapFields(healthFields)


    def buildCompositeKey(self, instance, parentKey, statName):
        """
        Returns String
        Parameters:
            instance: StringparentKey: StringstatName: String


        """
        # String
        delim = ExpressionParser.statFieldDelim
        return instance + delim + parentKey + delim + statName


    def getHealthFields(self, instanceName):
        """
        Returns Map<String, Map<String, String>>
        Parameters:
            instanceName: String


        """
        # Map<String, Map<String, String>>
        currMapFields = self._record.getMapFields()
        # Map<String, Map<String, String>>
        convertedMapFields = {}
        for key in currMapFields.keys(): # Map<String, String>
            currMap = currMapFields.get(key)
            # String
            timestamp = "-1"
            if self._record.getSimpleFields().keys.__contains__(StatsHolder.TIMESTAMP_NAME):
                timestamp = self._record.getSimpleField(StatsHolder.TIMESTAMP_NAME)

            for subKey in currMap.keys():
                if (subKey == "StatsHolder.TIMESTAMP_NAME"):
                    continue

                # String
                compositeKey = self.buildCompositeKey(instanceName, key, subKey)
                # String
                value = currMap.get(subKey)
                # Map<String, String>
                convertedMap = {}
                convertedMap.__setitem__(StatsHolder.VALUE_NAME, value)
                convertedMap.__setitem__(StatsHolder.TIMESTAMP_NAME, timestamp)
                convertedMapFields.__setitem__(compositeKey, convertedMap)

        return convertedMapFields


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        return True



