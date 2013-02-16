# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import HashMap
#from java.util import Map
#from java.util import TreeMap
#from org.apache.log4j import Logger
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.ZNRecord import ZNRecord

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum

CurrentStateProperty = enum('SESSION_ID', 'CURRENT_STATE', 'STATE_MODEL_DEF', 'STATE_MODEL_FACTORY_NAME', 'RESOURCE')

class CurrentState(HelixProperty):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)



    def __init__(self, *args):
        super(CurrentState,self).__init__(*args)
#        if len(args)==1 and (isinstance(args[0],str)  or isinstance((args[0]))
#        else:
#            raise IllegalArgumentException("Input arguments not supported. args = %s" % args)
#
#    """
#
#    Parameters:
#        String resourceName
#    """
#    def __init__(self, resourceName):
#        super(resourceName)
#
#
#    """
#
#    Parameters:
#        ZNRecord record
#    """
#    def __init__(self, record):
#        super(record)


    def getResourceName(self):
        """
        Returns String


        """
        return self._record.getId()


    def getPartitionStateMap(self):
        """
        Returns Map<String, String>


        """
        # Map<String, String>
        map = {}
        # Map<String, Map<String, String>>
        mapFields = self._record.getMapFields()
        for partitionName in mapFields.keys(): # Map<String, String>
            tempMap = mapFields.get(partitionName)
            if tempMap != None: 
                map.__setitem__(partitionName, tempMap.get(CurrentStateProperty.toString(CurrentStateProperty.CURRENT_STATE)))


        return map


    def getSessionId(self):
        """
        Returns String


        """
        return self._record.getSimpleField(CurrentStateProperty.toString(CurrentStateProperty.SESSION_ID))


    def setSessionId(self, sessionId):
        """
        Returns void
        Parameters:
            sessionId: String


        """
        self._record.setSimpleField(CurrentStateProperty.toString(CurrentStateProperty.SESSION_ID), sessionId)


    def getState(self, partitionName):
        """
        Returns String
        Parameters:
            partitionName: String


        """
        # Map<String, Map<String, String>>
        mapFields = self._record.getMapFields()
        # Map<String, String>
        mapField = mapFields.get(partitionName)
        if mapField != None: 
            return mapField.get(CurrentStateProperty.toString(CurrentStateProperty.CURRENT_STATE))

        return None


    def setStateModelDefRef(self, stateModelName):
        """
        Returns void
        Parameters:
            stateModelName: String


        """
        self._record.setSimpleField(CurrentStateProperty.toString(CurrentStateProperty.STATE_MODEL_DEF), stateModelName)


    def getStateModelDefRef(self):
        """
        Returns String


        """
        return self._record.getSimpleField(CurrentStateProperty.toString(CurrentStateProperty.STATE_MODEL_DEF))


    def setState(self, partitionName, state):
        """
        Returns void
        Parameters:
            partitionName: Stringstate: String


        """
        # Map<String, Map<String, String>>
        mapFields = self._record.getMapFields()
        if mapFields.get(partitionName) == None: 
            mapFields.__setitem__(partitionName, {})
#            mapFields.put(partitionName, TreeMap<String, String>())

        mapFields.get(partitionName).__setitem__(CurrentStateProperty.toString(CurrentStateProperty.CURRENT_STATE), state)


    def setStateModelFactoryName(self, factoryName):
        """
        Returns void
        Parameters:
            factoryName: String


        """
        self._record.setSimpleField(CurrentStateProperty.toString(CurrentStateProperty.STATE_MODEL_FACTORY_NAME), factoryName)


    def getStateModelFactoryName(self):
        """
        Returns String


        """
        return self._record.getSimpleField(CurrentStateProperty.toString(CurrentStateProperty.STATE_MODEL_FACTORY_NAME))


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        if self.getStateModelDefRef() == None:
            self.LOG.error("Current state does not contain state model ref. id:" + str(self.getResourceName()))
            return False

        if self.getSessionId() == None:
            self.LOG.error("CurrentState does not contain session id, id : " + str(self.getResourceName()))
            return False

        return True



