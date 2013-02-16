# package org.apache.helix
#from org.apache.helix import *
#from java.util import List
# do not include controller stuff: dzhang
#from org.apache.helix.controller.GenericHelixController import GenericHelixController

#from org.apache.helix.healthcheck.ParticipantHealthReportCollector import ParticipantHealthReportCollector
#from org.apache.helix.participant.HelixStateMachineEngine import HelixStateMachineEngine
#from org.apache.helix.participant.StateMachineEngine import StateMachineEngine
#from org.apache.helix.spectator.RoutingTableProvider import RoutingTableProvider
#from org.apache.helix.store.PropertyStore import PropertyStore
#from org.apache.helix.store.zk.ZkHelixPropertyStore import ZkHelixPropertyStore


class HelixManager:

    def connect(self):
        """
        Returns void


        Throws: 
            Exception
        """
        pass


    def isConnected(self):
        """
        Returns boolean


        """
        pass


    def disconnect(self):
        """
        Returns void


        """
        pass


    def addIdealStateChangeListener(self, listener):
        """
        Returns void
        Parameters:
            listener: IdealStateChangeListener


        Throws: 
            Exception
        """
        pass


    def addLiveInstanceChangeListener(self, listener):
        """
        Returns void
        Parameters:
            listener: LiveInstanceChangeListener


        Throws: 
            Exception
        """
        pass


    def addConfigChangeListener(self, listener):
        """
        Returns void
        Parameters:
            listener: ConfigChangeListener


        Throws: 
            Exception
        """
        pass


    def addMessageListener(self, listener, instanceName):
        """
        Returns void
        Parameters:
            listener: MessageListenerinstanceName: String


        Throws: 
            Exception
        """
        pass


    def addCurrentStateChangeListener(self, listener, instanceName, sessionId):
        """
        Returns void
        Parameters:
            listener: CurrentStateChangeListenerinstanceName: StringsessionId: String


        Throws: 
            Exception
        """
        pass


    def addHealthStateChangeListener(self, listener, instanceName):
        """
        Returns void
        Parameters:
            listener: HealthStateChangeListenerinstanceName: String


        Throws: 
            Exception
        """
        pass


    def addExternalViewChangeListener(self, listener):
        """
        Returns void
        Parameters:
            listener: ExternalViewChangeListener


        Throws: 
            Exception
        """
        pass


    def addControllerListener(self, listener):
        """
        Returns void
        Parameters:
            listener: ControllerChangeListener


        """
        pass


    def removeListener(self, listener):
        """
        Returns boolean
        Parameters:
            listener: Object


        """
        pass


    def getDataAccessor(self):
        """
        Returns DataAccessor
        @Deprecated


        """
        pass


    def getHelixDataAccessor(self):
        """
        Returns HelixDataAccessor


        """
        pass


    def getConfigAccessor(self):
        """
        Returns ConfigAccessor


        """
        pass


    def getClusterName(self):
        """
        Returns String


        """
        pass


    def getInstanceName(self):
        """
        Returns String


        """
        pass


    def getSessionId(self):
        """
        Returns String


        """
        pass


    def getLastNotificationTime(self):
        """
        Returns long


        """
        pass


    def getClusterManagmentTool(self):
        """
        Returns HelixAdmin


        """
        pass


    def getPropertyStore(self):
        """
        Returns PropertyStore<ZNRecord>
        @Deprecated


        """
        pass


    def getHelixPropertyStore(self):
        """
        Returns ZkHelixPropertyStore<ZNRecord>


        """
        pass


    def getMessagingService(self):
        """
        Returns ClusterMessagingService


        """
        pass


    def getHealthReportCollector(self):
        """
        Returns ParticipantHealthReportCollector


        """
        pass


    def getInstanceType(self):
        """
        Returns InstanceType


        """
        pass


    def getVersion(self):
        """
        Returns String


        """
        pass


    def getStateMachineEngine(self):
        """
        Returns StateMachineEngine


        """
        pass


    def isLeader(self):
        """
        Returns boolean


        """
        pass


    def startTimerTasks(self):
        """
        Returns void


        """
        pass


    def stopTimerTasks(self):
        """
        Returns void


        """
        pass


    def addPreConnectCallback(self, callback):
        """
        Returns void
        Parameters:
            callback: PreConnectCallback


        """
        pass



