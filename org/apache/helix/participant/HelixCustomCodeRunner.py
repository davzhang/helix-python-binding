# package org.apache.helix.participant
#from org.apache.helix.participant import *
#from java.util import ArrayList
#from java.util import Arrays
#from java.util import List
#from org.apache.log4j import Logger
from org.apache.helix.HelixConstants import ChangeType
from org.apache.helix.HelixConstants import StateModelToken
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.PropertyKey import PropertyKey
from org.apache.helix.manager.zk.ZKHelixDataAccessor import ZKHelixDataAccessor
from org.apache.helix.manager.zk.ZNRecordSerializer import ZNRecordSerializer
from org.apache.helix.manager.zk.ZkBaseDataAccessor import ZkBaseDataAccessor
from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.model.IdealState import IdealState
from org.apache.helix.model.IdealState import IdealStateModeProperty
from org.apache.helix.participant.GenericLeaderStandbyStateModelFactory import GenericLeaderStandbyStateModelFactory
from org.apache.helix.util.UserExceptions import IllegalArgumentException

from org.apache.helix.util.logger import get_logger

class HelixCustomCodeRunner:

    """
    Java modifiers:
         private final static
    Type:
        String
    """
    LEADER_STANDBY = "LeaderStandby"

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)

    """
    Java modifiers:
         private static
    Type:
        String
    """
    PARTICIPANT_LEADER = "PARTICIPANT_LEADER"

    """

    Parameters:
        HelixManager manager
        String zkAddr
    """
    def __init__(self, manager, zkAddr):
        self._manager = manager
        self._zkAddr = zkAddr


    def invoke(self, callback):
        """
        Returns HelixCustomCodeRunner
        Parameters:
            callback: CustomCodeCallbackHandler


        """
        self._callback = callback
        return self


    def on(self, notificationTypes):
        """
        Returns HelixCustomCodeRunner
        Parameters:
            notificationTypes: ChangeType


        """
        self._notificationTypes = notificationTypes
        return self


    def usingLeaderStandbyModel(self, id):
        """
        Returns HelixCustomCodeRunner
        Parameters:
            id: String


        """
        self._resourceName = HelixCustomCodeRunner.PARTICIPANT_LEADER + "_" + id
        return self


    def start(self):
        """
        Returns void


        Throws: 
            Exception
        """
        if self._callback == None or self._notificationTypes == None or self._notificationTypes.size() == 0 or self._resourceName == None:
            raise IllegalArgumentException("Require callback | notificationTypes | resourceName")


        self.LOG.info("Register participantLeader on " + self._notificationTypes + " using " + self._resourceName)
        _stateModelFty = GenericLeaderStandbyStateModelFactory(self._callback, self._notificationTypes)
        # StateMachineEngine
        stateMach = self._manager.getStateMachineEngine()
        stateMach.registerStateModelFactory(HelixCustomCodeRunner.LEADER_STANDBY, _stateModelFty, self._resourceName)
        # ZkClient
        zkClient = None
        try:
            zkClient = ZkClient(self._zkAddr, ZkClient.DEFAULT_CONNECTION_TIMEOUT)
            zkClient.setZkSerializer(ZNRecordSerializer())
            # HelixDataAccessor
            accessor = ZKHelixDataAccessor(self._manager.getClusterName(), ZkBaseDataAccessor(zkClient))
            # Builder
            keyBuilder = accessor.keyBuilder()
            # IdealState
            idealState = IdealState(self._resourceName)
            idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString())
            idealState.setReplicas(StateModelToken.ANY_LIVEINSTANCE.toString())
            idealState.setNumPartitions(1)
            idealState.setStateModelDefRef(HelixCustomCodeRunner.LEADER_STANDBY)
            idealState.setStateModelFactoryName(self._resourceName)
            # List<String>
            prefList = [StateModelToken.toString(StateModelToken.ANY_LIVEINSTANCE)]
            idealState.getRecord().setListField(self._resourceName + "_0", prefList)
            # List<String>
            idealStates = accessor.getChildNames(keyBuilder.idealStates())
            while (idealStates == None or not idealStates.__contains__(self._resourceName)):
                accessor.setProperty(keyBuilder.idealStates(self._resourceName), idealState)
                idealStates = accessor.getChildNames(keyBuilder.idealStates())

            self.LOG.info("Set idealState for participantLeader:" + self._resourceName + ", idealState:" + idealState)
        finally:
                if zkClient != None and zkClient.getConnection() != None:
                    zkClient.close()


    def stop(self):
        """
        Returns void


        """
        self.LOG.info("Removing stateModelFactory for " + self._resourceName)
        self._manager.getStateMachineEngine().removeStateModelFactory(HelixCustomCodeRunner.LEADER_STANDBY, self._stateModelFty, self._resourceName)



