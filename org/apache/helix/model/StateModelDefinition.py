# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.HelixProperty import HelixProperty

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum

StateModelDefinitionProperty=enum('INITIAL_STATE', 'STATE_TRANSITION_PRIORITYLIST', 'STATE_PRIORITY_LIST')


class StateModelDefinition(HelixProperty):


    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    _logger = get_logger(__name__)

    """

    Parameters:
        ZNRecord record
    """
    def __init__(self, record):
        super(StateModelDefinition,self).__init__(record)
        self._statesPriorityList = record.getListField(StateModelDefinitionProperty.toString(StateModelDefinitionProperty.STATE_PRIORITY_LIST))
        self._stateTransitionPriorityList = record.getListField(StateModelDefinitionProperty.toString(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST))
        self._stateTransitionTable = {}
#        self._stateTransitionTable = HashMap<String, Map<String, String>>()
        self._statesCountMap = {}
#        self_statesCountMap = HashMap<String, String>()
        if self._statesPriorityList != None:
            for state in self._statesPriorityList: # Map<String, String>
                metaData = record.getMapField(state + ".meta")
                if metaData != None: 
                    if metaData.__getitem__("count") != None:
                        self._statesCountMap.__setitem__(state, metaData.get("count"))


                # Map<String, String>
                nextData = record.getMapField(state + ".next")
                self._stateTransitionTable.__setitem__(state, nextData)


    def getStateTransitionPriorityList(self):
        """
        Returns List<String>


        """
        return self._stateTransitionPriorityList


    def getStatesPriorityList(self):
        """
        Returns List<String>


        """
        return self._statesPriorityList


    def getNextStateForTransition(self, fromState, toState):
        """
        Returns String
        Parameters:
            fromState: StringtoState: String


        """
        # Map<String, String>
        map = self._stateTransitionTable.get(fromState)
        if map != None: 
            return map.get(toState)

        return None


    def getInitialState(self):
        """
        Returns String


        """
        return self._record.getSimpleField(StateModelDefinitionProperty.toString(StateModelDefinitionProperty.INITIAL_STATE))


    def getNumInstancesPerState(self, state):
        """
        Returns String
        Parameters:
            state: String


        """
        return self._statesCountMap.get(state)


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        if self.getInitialState() == None:
            self._logger.error("State model does not contain init state, statemodel:" + str(self._record.getId()))
            return False

        if self._record.getListField(StateModelDefinitionProperty.toString(StateModelDefinitionProperty.STATE_PRIORITY_LIST)) == None:
            self._logger.error("CurrentState does not contain StatesPriorityList, state model : " + str(self._record.getId()))
            return False

        return True



