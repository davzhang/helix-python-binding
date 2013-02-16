# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.ZNRecord import ZNRecord

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum

PersistentStatsProperty=enum('SESSION_ID', 'FIELDS')

class PersistentStats(HelixProperty):

    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    _logger = get_logger(__name__)



    """
    Java modifiers:
         final static
    Type:
        String
    """
    nodeName = "PersistentStats"

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


    def setSessionId(self, sessionId):
        """
        Returns void
        Parameters:
            sessionId: String


        """
        self._record.setSimpleField(PersistentStatsProperty.SESSION_ID.toString(), sessionId)


    def getSessionId(self):
        """
        Returns String


        """
        return self._record.getSimpleField(PersistentStatsProperty.SESSION_ID.toString())


    def getInstanceName(self):
        """
        Returns String


        """
        return self._record.getId()


    def getMapFields(self):
        """
        Returns Map<String, Map<String, String>>


        """
        return self._record.getMapFields()


    def getStatFields(self, statName):
        """
        Returns Map<String, String>
        Parameters:
            statName: String


        """
        return self._record.getMapField(statName)


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        return True



