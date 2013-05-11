# package org.apache.helix.participant
#from org.apache.helix.participant import *
#from java.util import Map
#from java.util import UUID
#from java.util.concurrent import ConcurrentHashMap
#from org.apache.log4j import Logger
from org.apache.helix.HelixConstants import HelixConstants
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixException import HelixException
#from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.NotificationContext import NotificationContext
#from org.apache.helix.PropertyKey import Builder
# TODO: enable this
#from org.apache.helix.messaging.handling.HelixStateTransitionHandler import HelixStateTransitionHandler

#from org.apache.helix.messaging.handling.HelixTaskExecutor import HelixTaskExecutor
from org.apache.helix.messaging.handling.HelixStateTransitionHandler import HelixStateTransitionHandler
from org.apache.helix.messaging.handling.MessageHandler import MessageHandler
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageType
from org.apache.helix.model.StateModelDefinition import StateModelDefinition
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory
# TODO: enable this
#from org.apache.helix.participant.statemachine.StateModelParser import StateModelParser

from org.apache.helix.participant.StateMachineEngine import StateMachineEngine


import logging, uuid
from org.apache.helix.participant.statemachine.StateModelParser import StateModelParser
from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import ternary
from org.apache.helix.util.UserExceptions import UnsupportedOperationException

class HelixStateMachineEngine(StateMachineEngine):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
#    logger = Logger.getLogger(HelixStateMachineEngine.class)
    logger = get_logger(__name__)
    logger.setLevel(logging.INFO)


#    def getStateModelFactory(self, stateModelName):
#        """
#        Returns StateModelFactory<? extends StateModel>
#        Parameters:
#            stateModelName: String
#
#
#        """
#        return getStateModelFactory(stateModelName, HelixConstants.DEFAULT_STATE_MODEL_FACTORY)


    def getStateModelFactory(self, stateModelName, factoryName=HelixConstants.DEFAULT_STATE_MODEL_FACTORY):
        """
        Returns StateModelFactory<? extends StateModel>
        Parameters:
            stateModelName: StringfactoryName: String


        """
        if not self._stateModelFactoryMap.__contains__(stateModelName):
            return None

        return self._stateModelFactoryMap.get(stateModelName).get(factoryName)


    """

    Parameters:
        HelixManager manager
    """
    def __init__(self, manager):
        self._stateModelParser = StateModelParser()
        self._manager = manager
        self._stateModelFactoryMap = {}
#        self._stateModelFactoryMap = ConcurrentHashMap<String, Map<String, StateModelFactory<? extends StateModel>>>()
        self._stateModelDefs = {}
#        self._stateModelDefs = ConcurrentHashMap<String, StateModelDefinition>()


    def registerStateModelFactory(self, stateModelName, factory, factoryName=HelixConstants.DEFAULT_STATE_MODEL_FACTORY):
        """
        Returns boolean
        Parameters:
            stateModelName: Stringfactory: StateModelFactory<? extends StateModel>factoryName: String
        @Override


        """
        if stateModelName == None or factory == None or factoryName == None: 
            raise HelixException("stateModelDef|stateModelFactory|factoryName cannot be null")


        self.logger.info("Register state model factory for state model " + stateModelName + " using factory name " + factoryName + " with " + str(factory))
        if not self._stateModelFactoryMap.__contains__(stateModelName):
            self._stateModelFactoryMap.__setitem__(stateModelName, {})
#            self._stateModelFactoryMap.__setitem__(stateModelName, ConcurrentHashMap<String, StateModelFactory<? extends StateModel>>())

        if self._stateModelFactoryMap.get(stateModelName).__contains__(factoryName):
            self.logger.warn("stateModelFactory for " + stateModelName + " using factoryName " + factoryName + " has already been registered.")
            return False

        self._stateModelFactoryMap.get(stateModelName).__setitem__(factoryName, factory)
        self.sendNopMessage()
        return True


    def sendNopMessage(self):
        """
        Returns void
        Java modifiers:
             private

        """
        if self._manager.isConnected():
            try:
                # Message
                nopMsg = Message(MessageType.NO_OP, str(uuid.uuid4()))
                nopMsg.setSrcName(self._manager.getInstanceName())
                # HelixDataAccessor
                accessor = self._manager.getHelixDataAccessor()
                # Builder
                keyBuilder = accessor.keyBuilder()
                if self._manager.getInstanceType() == InstanceType.CONTROLLER or self._manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT:
                    nopMsg.setTgtName("Controller")
                    accessor.setProperty(keyBuilder.controllerMessage(nopMsg.getId()), nopMsg)

                if self._manager.getInstanceType() == InstanceType.PARTICIPANT or self._manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT:
                    nopMsg.setTgtName(self._manager.getInstanceName())
                    accessor.setProperty(keyBuilder.message(nopMsg.getTgtName(), nopMsg.getId()), nopMsg)

                self.logger.info("Send NO_OP message to " + nopMsg.getTgtName() + ", msgId: " + nopMsg.getId())
            except Exception, e:
                self.logger.error(e)




    def reset(self):
        """
        Returns void
        @Override


        """
        for ftyMap in self._stateModelFactoryMap.values():
            for stateModelFactory in ftyMap.values(): # Map<String, ? extends StateModel>
                modelMap = stateModelFactory.getStateModelMap()
#                if modelMap == None or modelMap.isEmpty():
                if not modelMap:
                    continue

                for resourceKey in modelMap.keys(): # StateModel
                    stateModel = modelMap.get(resourceKey)
                    stateModel.reset()
                    # String
                    initialState = self._stateModelParser.getInitialState(stateModel.__class__)
                    stateModel.updateState(initialState)


    def createHandler(self, message, context):
        """
        Returns MessageHandler
        Parameters:
            message: Messagecontext: NotificationContext
        @Override


        """
        # String
        type = message.getMsgType()
        if not (type == MessageType.toString(MessageType.STATE_TRANSITION)):
            raise HelixException("Unexpected msg type for message " + message.getMsgId() + " type:" + message.getMsgType())


        # String
        partitionKey = message.getPartitionName()
        # String
        stateModelName = message.getStateModelDef()
        # String
        resourceName = message.getResourceName()
        # String
        sessionId = message.getTgtSessionId()
        # int
        bucketSize = message.getBucketSize()
        if stateModelName == None: 
            self.logger.error("message does not contain stateModelDef")
            return None

        # String
        factoryName = message.getStateModelFactoryName()
        if factoryName == None: 
            factoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY

        # StateModelFactory
        stateModelFactory = self.getStateModelFactory(stateModelName, factoryName)
        if stateModelFactory == None: 
            self.logger.warn("Cannot find stateModelFactory for model:" + stateModelName + " using factoryName:" + factoryName + " for resourceGroup:" + resourceName)
            return None

        if not self._stateModelDefs.__contains__(stateModelName):
            # HelixDataAccessor
            accessor = self._manager.getHelixDataAccessor()
            # Builder
            keyBuilder = accessor.keyBuilder()
            # StateModelDefinition
            stateModelDef = accessor.getProperty(keyBuilder.stateModelDef(stateModelName))
            if stateModelDef == None: 
                raise HelixException("stateModelDef for " + stateModelName + " does NOT exists")


            self._stateModelDefs.__setitem__(stateModelName, stateModelDef)

        # String
        initState = self._stateModelDefs.get(message.getStateModelDef()).getInitialState()
        # StateModel
        stateModel = stateModelFactory.getStateModel(partitionKey)
        if stateModel == None: 
            stateModelFactory.createAndAddStateModel(partitionKey)
            stateModel = stateModelFactory.getStateModel(partitionKey)
            stateModel.updateState(initState)

        # CurrentState
        currentStateDelta = CurrentState(resourceName)
        currentStateDelta.setSessionId(sessionId)
        currentStateDelta.setStateModelDefRef(stateModelName)
        currentStateDelta.setStateModelFactoryName(factoryName)
        currentStateDelta.setBucketSize(bucketSize)
        currentStateDelta.setState(partitionKey, ternary((stateModel.getCurrentState() == None), initState, stateModel.getCurrentState()))
        # HelixTaskExecutor
        executor = context.get(NotificationContext.TASK_EXECUTOR_KEY)
#        executor = (HelixTaskExecutor) context.get(NotificationContext.TASK_EXECUTOR_KEY)
        return HelixStateTransitionHandler(stateModel, message, context, currentStateDelta, executor)


    def getMessageType(self):
        """
        Returns String
        @Override


        """
        return MessageType.toString(MessageType.STATE_TRANSITION)


#    def removeStateModelFactory(self, stateModelDef, factory):
#        """
#        Returns boolean
#        Parameters:
#            stateModelDef: Stringfactory: StateModelFactory<? extends StateModel>
#        @Override
#
#
#        """
#        raise UnsupportedOperationException("Remove not yet supported")
#
#
#
#    def removeStateModelFactory(self, stateModelDef, factory, factoryName):
#        """
#        Returns boolean
#        Parameters:
#            stateModelDef: Stringfactory: StateModelFactory<? extends StateModel>factoryName: String
#        @Override
#
#
#        """
#        raise UnsupportedOperationException("Remove not yet supported")




