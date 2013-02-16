# package org.apache.helix.messaging
#from org.apache.helix.messaging import *
#from java.util import ArrayList
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from java.util import UUID
#from java.util.concurrent import ConcurrentHashMap
#from org.apache.log4j import Logger
import uuid
from org.apache.helix.ClusterMessagingService import ClusterMessagingService
from org.apache.helix.ConfigAccessor import ConfigAccessor
from org.apache.helix.ConfigScope import ConfigScope
from org.apache.helix.ConfigScopeBuilder import ConfigScopeBuilder
from org.apache.helix.Criteria import Criteria
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.InstanceType import InstanceType
#from org.apache.helix.PropertyKey import Builder
from org.apache.helix.messaging.CriteriaEvaluator import CriteriaEvaluator
from org.apache.helix.messaging.handling.AsyncCallbackService import AsyncCallbackService
from org.apache.helix.messaging.handling.HelixTaskExecutor import HelixTaskExecutor
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.model.LiveInstance import LiveInstance
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageType

from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum

class DefaultMessagingService(ClusterMessagingService):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    _logger = get_logger(__name__)
    """

    Parameters:
        HelixManager manager
    """
    def __init__(self, manager):
        self._manager = manager
        self._evaluator = CriteriaEvaluator()
        self._taskExecutor = HelixTaskExecutor()
        self._asyncCallbackService = AsyncCallbackService()
        self._taskExecutor.registerMessageHandlerFactory(MessageType.toString(MessageType.TASK_REPLY), self._asyncCallbackService)
        self._messageHandlerFactoriestobeAdded={}  #ConcurrentHashMap<String, MessageHandlerFactory>

#    def send(self, recipientCriteria, messageTemplate):
#        """
#        Returns int
#        Parameters:
#            recipientCriteria: CriteriamessageTemplate: Message
#        @Override
#
#
#        """
#        return self.send(recipientCriteria, messageTemplate, None, -1)
#
#
#    def send(self, recipientCriteria, message, callbackOnReply, timeOut):
#        """
#        Returns int
#        Parameters:
#            recipientCriteria: Criteriamessage: MessagecallbackOnReply: AsyncCallbacktimeOut: int
#        @Override
#
#
#        """
#        return self.send(recipientCriteria, message, callbackOnReply, timeOut, 0)
#
#
    def send(self, recipientCriteria, message, callbackOnReply=None, timeOut=-1, retryCount=0):
        """
        Returns int
        Parameters:
            recipientCriteria: Criteriamessage: MessagecallbackOnReply: AsyncCallbacktimeOut: intretryCount: int
        @Override


        """
        # Map<InstanceType, List<Message>>
        generateMessage = self.generateMessage(recipientCriteria, message)
        # int
        totalMessageCount = 0
        for messages  in generateMessage.values():
            totalMessageCount += messages.size()

        self._logger.info("Send " + totalMessageCount + " messages with criteria " + recipientCriteria)
        if totalMessageCount == 0: 
            return 0

        # String
        correlationId = None
        if callbackOnReply != None: 
            # int
            totalTimeout = timeOut * (retryCount + 1)
            if totalTimeout < 0: 
                totalTimeout = -1

            callbackOnReply.setTimeout(totalTimeout)
#            correlationId = UUID.randomUUID().toString()
            correlationId = str(uuid.uuid4())
            for  messages in generateMessage.values():
                callbackOnReply.setMessagesSent(messages)

            self._asyncCallbackService.registerAsyncCallback(correlationId, callbackOnReply)

        for receiverType in generateMessage.keys(): # List<Message>
            list = generateMessage.get(receiverType)
            for tempMessage in list:
                tempMessage.setRetryCount(retryCount)
                tempMessage.setExecutionTimeout(timeOut)
                tempMessage.setSrcInstanceType(self._manager.getInstanceType())
                if correlationId != None: 
                    tempMessage.setCorrelationId(correlationId)

                # HelixDataAccessor
                accessor = self._manager.getHelixDataAccessor()
                # Builder
                keyBuilder = accessor.keyBuilder()
                if receiverType == InstanceType.CONTROLLER: 
                    accessor.setProperty(keyBuilder.controllerMessage(tempMessage.getId()), tempMessage)

                if receiverType == InstanceType.PARTICIPANT: 
                    accessor.setProperty(keyBuilder.message(tempMessage.getTgtName(), tempMessage.getId()), tempMessage)



        if callbackOnReply != None: 
            callbackOnReply.startTimer()

        return totalMessageCount


    def generateMessage(self, recipientCriteria, message):
        """
        Returns Map<InstanceType, List<Message>>
        Parameters:
            recipientCriteria: Criteriamessage: Message
        Java modifiers:
             private

        """
        # Map<InstanceType, List<Message>>
        messagesToSendMap = {}
#        messagesToSendMap = HashMap<InstanceType, List<Message>>()
        # InstanceType
        instanceType = recipientCriteria.getRecipientInstanceType()
        if instanceType == InstanceType.CONTROLLER: 
            # List<Message>
            messages = self.generateMessagesForController(message)
            messagesToSendMap.put(InstanceType.CONTROLLER, messages)
        else:
            if instanceType == InstanceType.PARTICIPANT: 
                # List<Message>
                messages = []
#                messages = ArrayList<Message>()
                # List<Map<String, String>>
                matchedList = self._evaluator.evaluateCriteria(recipientCriteria, self._manager)
                if not matchedList.isEmpty(): 
                    # Map<String, String>
                    sessionIdMap = {}
#                    sessionIdMap = HashMap<String, String>()
                    if recipientCriteria.isSessionSpecific():
                        # HelixDataAccessor
                        accessor = self._manager.getHelixDataAccessor()
                        # Builder
                        keyBuilder = accessor.keyBuilder()
                        # List<LiveInstance>
                        liveInstances = accessor.getChildValues(keyBuilder.liveInstances())
                        for liveInstance in liveInstances:
                            sessionIdMap.__setitem__(liveInstance.getInstanceName(), liveInstance.getSessionId())


                    for map in matchedList:
                        id = str(uuid.uuid4())
                        # Message
                        newMessage = Message(message.getRecord(), id)
                        # String
                        srcInstanceName = self._manager.getInstanceName()
                        # String
                        tgtInstanceName = map.get("instanceName")
                        if recipientCriteria.isSelfExcluded() and srcInstanceName.upper() == tgtInstanceName.upper():
                            continue

                        newMessage.setSrcName(srcInstanceName)
                        newMessage.setTgtName(tgtInstanceName)
                        newMessage.setResourceName(map.get("resourceName"))
                        newMessage.setPartitionName(map.get("partitionName"))
                        if recipientCriteria.isSessionSpecific(): 
                            newMessage.setTgtSessionId(sessionIdMap.get(tgtInstanceName))

                        messages.append(newMessage)

                    messagesToSendMap.__setitem__(InstanceType.PARTICIPANT, messages)


        return messagesToSendMap


    def generateMessagesForController(self, message):
        """
        Returns List<Message>
        Parameters:
            message: Message
        Java modifiers:
             private

        """
        # List<Message>
#        messages = ArrayList<Message>()
        messages = []
        # String
        id = str(uuid.uuid4())
        # Message
        newMessage = Message(message.getRecord(), id)
        newMessage.setMsgId(id)
        newMessage.setSrcName(self._manager.getInstanceName())
        newMessage.setTgtName("Controller")
        messages.add(newMessage)
        return messages


    def registerMessageHandlerFactory(self, type, factory):
        """
        Returns void
        Parameters:
            type: Stringfactory: MessageHandlerFactory
        @Override
        Java modifiers:
             synchronized

        """
        if self._manager.isConnected(): 
            self.registerMessageHandlerFactoryInternal(type, factory)
        else:
            self._messageHandlerFactoriestobeAdded.put(type, factory)



    def onConnected(self):
        """
        Returns void
        Java modifiers:
             synchronized

        """
        for type in self._messageHandlerFactoriestobeAdded.keys():
            self.registerMessageHandlerFactoryInternal(type, self._messageHandlerFactoriestobeAdded.get(type))

        self._messageHandlerFactoriestobeAdded.clear()


    def registerMessageHandlerFactoryInternal(self, type, factory):
        """
        Returns void
        Parameters:
            type: Stringfactory: MessageHandlerFactory


        """
        self._logger.info("registering msg factory for type " + type)
        # int
        threadpoolSize = HelixTaskExecutor.DEFAULT_PARALLEL_TASKS
        # String
        threadpoolSizeStr = None
        # String
        key = type + "." + HelixTaskExecutor.MAX_THREADS
        # ConfigAccessor
        configAccessor = self._manager.getConfigAccessor()
        if configAccessor != None: 
            # ConfigScope
            scope = None
            if self._manager.getInstanceType() == InstanceType.PARTICIPANT or self._manager.getInstanceType() == InstanceType.CONTROLLER_PARTICIPANT: 
                scope = ConfigScopeBuilder().forCluster(self._manager.getClusterName()).forParticipant(self._manager.getInstanceName()).build()
                threadpoolSizeStr = configAccessor.get(scope, key)

            if threadpoolSizeStr == None: 
                scope = ConfigScopeBuilder().forCluster(self._manager.getClusterName()).build()
                threadpoolSizeStr = configAccessor.get(scope, key)


        if threadpoolSizeStr != None: 
            try:
                threadpoolSize = int(threadpoolSizeStr)
                if threadpoolSize <= 0: 
                    threadpoolSize = 1

            except Exception, e:
                self._logger.error(""+ str(e))


        self._taskExecutor.registerMessageHandlerFactory(type, factory, threadpoolSize)
        self.sendNopMessage()


    def sendNopMessage(self):
        """
        Returns void


        """
        if self._manager.isConnected():
            # TODO: enable the try/catch
#            try:
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

#            except Exception, e:
#                self._logger.error(e)




    def getExecutor(self):
        """
        Returns HelixTaskExecutor


        """
        return self._taskExecutor


    def sendAndWait(self, receipientCriteria, message, asyncCallback, timeOut, retryCount=0):
        """
        Returns int
        Parameters:
            receipientCriteria: Criteriamessage: MessageasyncCallback: AsyncCallbacktimeOut: intretryCount: int
        @Override


        """
        # int
        messagesSent = self.send(receipientCriteria, message, asyncCallback, timeOut, retryCount)
        if messagesSent > 0: 
            while not asyncCallback.isDone() and not asyncCallback.isTimedOut():
            # TODO
#                synchronized (asyncCallback)
                try:
                    asyncCallback.wait()
#                except InterruptedException, e:
                except Exception, e:
                    self._logger.error(e)
                    asyncCallback.setInterrupted(True)
                    break

        else:
            self._logger.warn("No messages sent. For Criteria:" + receipientCriteria)

        return messagesSent


#    def sendAndWait(self, recipientCriteria, message, asyncCallback, timeOut):
#        """
#        Returns int
#        Parameters:
#            recipientCriteria: Criteriamessage: MessageasyncCallback: AsyncCallbacktimeOut: int
#        @Override
#
#
#        """
#        return self.sendAndWait(recipientCriteria, message, asyncCallback, timeOut, 0)



