# package org.apache.helix.model
#from org.apache.helix.model import *
#from org.apache.log4j import Logger
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.HelixProperty import HelixProperty

from org.apache.helix.util.misc import enum
from org.apache.helix.util.logger import get_logger

LiveInstanceProperty=enum('SESSION_ID', 'HELIX_VERSION', 'LIVE_INSTANCE', 'ZKPROPERTYTRANSFERURL')

class LiveInstance(HelixProperty):



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
    def __init__(self, *args):
        super(LiveInstance,self).__init__(*args)

#    def __init__(self, id):
#        super(id)
#
#    """
#
#    Parameters:
#        ZNRecord record
#    """
#    def __init__(self, record):
#        super(record)


    def setSessionId(self, sessionId):
        """
        Returns void
        Parameters:
            sessionId: String


        """
        self._record.setSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.SESSION_ID), sessionId)


    def getSessionId(self):
        """
        Returns String


        """
        return self._record.getSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.SESSION_ID))


    def getInstanceName(self):
        """
        Returns String


        """
        return self._record.getId()


    def getHelixVersion(self):
        """
        Returns String


        """
        return self._record.getSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.HELIX_VERSION))


    def setHelixVersion(self, helixVersion):
        """
        Returns void
        Parameters:
            helixVersion: String


        """
        self._record.setSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.HELIX_VERSION), helixVersion)


    def getLiveInstance(self):
        """
        Returns String


        """
        return self._record.getSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.LIVE_INSTANCE))


    def setLiveInstance(self, leader):
        """
        Returns void
        Parameters:
            leader: String


        """
        self._record.setSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.LIVE_INSTANCE), leader)


    def getModifiedTime(self):
        """
        Returns long


        """
        return self._record.getModifiedTime()


    def getWebserviceUrl(self):
        """
        Returns String


        """
        return self._record.getSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.ZKPROPERTYTRANSFERURL))


    def setWebserviceUrl(self, url):
        """
        Returns void
        Parameters:
            url: String


        """
        self._record.setSimpleField(LiveInstanceProperty.toString(LiveInstanceProperty.ZKPROPERTYTRANSFERURL), url)


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        if self.getSessionId() == None:
            self._logger.error("liveInstance does not have session id. id:" + str(self._record.getId()))
            return False

        if self.getHelixVersion() == None:
            self._logger.error("liveInstance does not have CLM verion. id:" + str(self._record.getId()))
            return False

        return True



