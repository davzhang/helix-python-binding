# package org.apache.helix.participant
#from org.apache.helix.participant import *
#from org.apache.log4j import Logger
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.HelixManagerFactory import HelixManagerFactory
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelInfo import StateModelInfo
from org.apache.helix.participant.statemachine.StateModelParser import StateModelParser
from org.apache.helix.participant.statemachine.StateTransitionError import StateTransitionError
from org.apache.helix.participant.statemachine.Transition import Transition

# Annotation @StateModelInfo(initialState = "OFFLINE", states = { "LEADER", "STANDBY" })


class DistClusterControllerStateModel(StateModel):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = Logger.getLogger(DistClusterControllerStateModel.class)





    """

    Parameters:
        String zkAddr
    """
    def __init__(self, zkAddr):
        # StateModelParser
        parser = StateModelParser()
        self._currentState = parser.getInitialState(DistClusterControllerStateModel.class)
        self._zkAddr = zkAddr


    def onBecomeStandbyFromOffline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "STANDBY", from = "OFFLINE")



        """
        logger.info("Becoming standby from offline")


    def onBecomeLeaderFromStandby(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "LEADER", from = "STANDBY")



        Throws: 
            Exception
        """
        # String
        clusterName = message.getPartitionName()
        # String
        controllerName = message.getTgtName()
        logger.info(controllerName + " becomes leader from standby for " + clusterName)
        if _controller == None: 
            _controller = HelixManagerFactory.getZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, _zkAddr)
            _controller.connect()
            _controller.startTimerTasks()
        else:
            logger.error("controller already exists:" + str(_controller.getInstanceName())+ " for " + str(clusterName))



    def onBecomeStandbyFromLeader(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "STANDBY", from = "LEADER")



        """
        # String
        clusterName = message.getPartitionName()
        # String
        controllerName = message.getTgtName()
        logger.info(controllerName + " becoming standby from leader for " + clusterName)
        if _controller != None: 
            _controller.disconnect()
            _controller = None
        else:
            logger.error("No controller exists for " + str(clusterName))



    def onBecomeOfflineFromStandby(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "OFFLINE", from = "STANDBY")



        """
        # String
        clusterName = message.getPartitionName()
        # String
        controllerName = message.getTgtName()
        logger.info(controllerName + " becoming offline from standby for cluster:" + clusterName)


    def onBecomeDroppedFromOffline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "DROPPED", from = "OFFLINE")



        """
        logger.info("Becoming dropped from offline")


    def onBecomeOfflineFromDropped(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "OFFLINE", from = "DROPPED")



        """
        logger.info("Becoming offline from dropped")


    def rollbackOnError(self, message, context, error):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContexterror: StateTransitionError
        @Override


        """
        # String
        clusterName = message.getPartitionName()
        # String
        controllerName = message.getTgtName()
        logger.error(controllerName + " rollbacks on error for " + str(clusterName))
        if _controller != None: 
            _controller.disconnect()
            _controller = None



    def reset(self):
        """
        Returns void
        @Override


        """
        if _controller != None: 
            _controller.disconnect()
            _controller = None




