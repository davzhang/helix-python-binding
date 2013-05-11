# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
#from java.lang.reflect import InvocationTargetException
#from java.lang.reflect import Method
#from java.util import ArrayList
#from java.util import Arrays
#from java.util import Date
#from java.util import List
#from org.apache.log4j import Logger
import time
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.PropertyKey import PropertyKey
#from org.apache.helix.PropertyKey import Builder
from org.apache.helix.ZNRecordBucketizer import ZNRecordBucketizer
from org.apache.helix.ZNRecordDelta import ZNRecordDelta
from org.apache.helix.ZNRecordDelta import MergeOperation
from org.apache.helix.messaging.handling.HelixTaskResult import HelixTaskResult
from org.apache.helix.messaging.handling.MessageHandler import MessageHandler, ErrorType, ErrorCode
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.Message import Message, Attributes
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelParser import StateModelParser
from org.apache.helix.participant.statemachine.StateTransitionError import StateTransitionError
from org.apache.helix.util.StatusUpdateUtil import StatusUpdateUtil

from org.apache.helix.util.logger import get_logger

class HelixStateMismatchException(Exception):

    """

    Parameters:
        String info
    """
    def __init__(self, info):
        super(info)


class HelixStateTransitionHandler(MessageHandler):



    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """

    Parameters:
        StateModel stateModel
        Message message
        NotificationContext context
        CurrentState currentStateDelta
        HelixTaskExecutor executor
    """
    def __init__(self, stateModel, message, context, currentStateDelta, executor):
        super(HelixStateTransitionHandler,self).__init__(message, context)
        self._stateModel = stateModel
        self._statusUpdateUtil = StatusUpdateUtil()
        self._transitionMethodFinder = StateModelParser()
        self._currentStateDelta = currentStateDelta
        self._executor = executor


    def prepareMessageExecution(self, manager, message):
        """
        Returns void
        Parameters:
            manager: HelixManagermessage: Message
        Java modifiers:
             private

        Throws: 
            HelixException
            HelixStateMismatchException
        """
        if not message.isValid(): 
            # String
#            errorMessage = "Invalid Message, ensure that message: " + message + " has all the required fields: " + str(Message.Attributes.values())
            errorMessage = "Invalid Message, ensure that message: " + message + " has all the required fields: " + str(Attributes.values())
            self._statusUpdateUtil.logError(message, HelixStateTransitionHandler, errorMessage, manager.getHelixDataAccessor())
            self.logger.error(errorMessage)
            raise HelixException(errorMessage)


        # HelixDataAccessor
        accessor = manager.getHelixDataAccessor()
        # String
        partitionName = message.getPartitionName()
        # String
        fromState = message.getFromState()
        # String
        state = self._currentStateDelta.getState(partitionName)
        if fromState != None and  (fromState == "*") and not fromState.upper()==state.upper():
            # String
            errorMessage = "Current state of stateModel does not match the fromState in Message" + ", Current State:" + str(state) + ", message expected:" + str(fromState) + ", partition: " + str(partitionName) + ", from: " + message.getMsgSrc() + ", to: " + message.getTgtName()
            self._statusUpdateUtil.logError(message, HelixStateTransitionHandler, errorMessage, accessor)
            self.logger.error(errorMessage)
            raise HelixStateMismatchException(errorMessage)




    def postExecutionMessage(self, manager, message, context, taskResult, exception):
        """
        Returns void
        Parameters:
            manager: HelixManagermessage: Messagecontext: NotificationContexttaskResult: HelixTaskResultexception: Exception


        """
        # String
        partitionKey = message.getPartitionName()
        # String
        resource = message.getResourceName()
        # String
        sessionId = message.getTgtSessionId()
        # String
        instanceName = manager.getInstanceName()
        # HelixDataAccessor
        accessor = manager.getHelixDataAccessor()
        # Builder
        keyBuilder = accessor.keyBuilder()
        # int
        bucketSize = message.getBucketSize()
        # ZNRecordBucketizer
        bucketizer = ZNRecordBucketizer(bucketSize)
        # TODO
#        synchronized (manager)
        if not (message.getTgtSessionId() == manager.getSessionId()):
            self.logger.warn("Session id has changed. Skip postExecutionMessage. Old session " + message.getExecutionSessionId() + " , new session : " + manager.getSessionId())
            return

        if taskResult.isSucess():
            # String
            toState = message.getToState()
            self._currentStateDelta.setState(partitionKey, toState)
            if toState.upper() == "DROPPED":
                # ZNRecordDelta
                delta = ZNRecordDelta(self._currentStateDelta.getRecord(), MergeOperation.SUBTRACT)
                delta._record.getSimpleFields().clear()
                # List<ZNRecordDelta>
                deltaList = []
                deltaList.append(delta)
                self._currentStateDelta.setDeltaList(deltaList)
            else:
                self._stateModel.updateState(toState)

        else:
            if type(exception) == HelixStateMismatchException:
                self.logger.warn("Force CurrentState on Zk to be stateModel's CurrentState. partitionKey: " + partitionKey + ", currentState: " + self._stateModel.getCurrentState() + ", message: " + message)
                self._currentStateDelta.setState(partitionKey, self._stateModel.getCurrentState())
            else:
                # StateTransitionError
                error = StateTransitionError(ErrorType.INTERNAL, ErrorCode.ERROR, exception)
                # TODO:
#                if type(exception) == InterruptedException:
#                    if _isTimeout:
#                        error = StateTransitionError(ErrorType.INTERNAL, ErrorCode.TIMEOUT, exception)
#                    else:
#                        self.logger.error("State transition interrupted but not timeout. Not updating state. Partition : " + str(message.getPartitionName())+ " MsgId : " + str(message.getMsgId()))
#                        return

                self._stateModel.rollbackOnError(message, context, error)
                self._currentStateDelta.setState(partitionKey, "ERROR")
                self._stateModel.updateState("ERROR")

        # synchronize manager

        try:
            # PropertyKey
            key = keyBuilder.currentState(instanceName, sessionId, resource, bucketizer.getBucketName(partitionKey))
            if not self._message.getGroupMessageMode(): 
                accessor.updateProperty(key, self._currentStateDelta)
            else:
                self._executor._groupMsgHandler.addCurStateUpdate(self._message, key, self._currentStateDelta)

        except Exception, e:
            self.logger.error("Error when updating the state "+ str(e))
            # StateTransitionError
            error = StateTransitionError(ErrorType.FRAMEWORK, ErrorCode.ERROR, e)
            self._stateModel.rollbackOnError(message, context, error)
            self._statusUpdateUtil.logError(message, HelixStateTransitionHandler, e, "Error when update the state ", accessor)



    def handleMessageInternal(self, message, context):
        """
        Returns HelixTaskResult
        Parameters:
            message: Messagecontext: NotificationContext


        """
       # synchronized (self._stateModel) # HelixTaskResult
        try:
            taskResult = HelixTaskResult()
            # HelixManager
            manager = context.getManager()
            # HelixDataAccessor
            accessor = manager.getHelixDataAccessor()
            self._statusUpdateUtil.logInfo(message, HelixStateTransitionHandler, "Message handling task begin execute", accessor)
            message.setExecuteStartTimeStamp(time.time())
            # Exception
            exception = None
            try:
                self.prepareMessageExecution(manager, message)
                self.invoke(accessor, context, taskResult, message)
            except HelixStateMismatchException, e:
                taskResult.setSuccess(False)
                taskResult.setMessage(str(e))
                taskResult.setException(e)
                exception = e
            except Exception, e:
                # String
                errorMessage = "Exception while executing a state transition task " + message.getPartitionName()
                self.logger.error(errorMessage+ str(e))
#                if e.getCause() != None and type(e.getCause()) == InterruptedException:
#                    e = (InterruptedException) e.getCause()

                self._statusUpdateUtil.logError(message, HelixStateTransitionHandler, e, errorMessage, accessor)
                taskResult.setSuccess(False)
                taskResult.setMessage(str(e))
                taskResult.setException(e)
#                taskResult.setInterrupted(type(e) == InterruptedException)
                exception = e

            self.postExecutionMessage(manager, message, context, taskResult, exception)
            return taskResult
        finally: pass



    def invoke(self, accessor, context, taskResult, message):
        """
        Returns void
        Parameters:
            accessor: HelixDataAccessorcontext: NotificationContexttaskResult: HelixTaskResultmessage: Message
        Java modifiers:
             private

        Throws: 
            IllegalAccessException
            InvocationTargetException
            InterruptedException
        """
        self._statusUpdateUtil.logInfo(message, HelixStateTransitionHandler, "Message handling invoking", accessor)
        # Method
        methodToInvoke = None
        # String
        fromState = message.getFromState()
        # String
        toState = message.getToState()
#        methodToInvoke = self._transitionMethodFinder.getMethodForTransition(self._stateModel.getClass(), fromState, toState, new Class[] { Message.class, NotificationContext.class })
        methodToInvoke = self._transitionMethodFinder.getMethodForTransition(self._stateModel, fromState, toState)
        if methodToInvoke != None:
            methodToInvoke(message, context)
#            methodToInvoke.self.invoke(self._stateModel, new Object[] { message, context })
            taskResult.setSuccess(True)
        else:
            # String
            errorMessage = "Unable to find method for transition from " + str(fromState) + " to " + str(toState) + "in " + str(self._stateModel)
            self.logger.error(errorMessage)
            taskResult.setSuccess(False)
            self._statusUpdateUtil.logError(message, HelixStateTransitionHandler, errorMessage, accessor)



    def handleMessage(self):
        """
        Returns HelixTaskResult
        @Override


        """
        return self.handleMessageInternal(self._message, self._notificationContext)


    def onError(self, e, code, type):
        """
        Returns void
        Parameters:
            e: Exceptioncode: ErrorCodetype: ErrorType
        @Override


        """
        if type == ErrorType.INTERNAL: 
            self.logger.error("Skip internal error " + str(e.getMessage())+ " " + str(code))
            return

        # HelixManager
        manager = self._notificationContext.getManager()
        # HelixDataAccessor
        accessor = manager.getHelixDataAccessor()
        # Builder
        keyBuilder = accessor.keyBuilder()
        # String
        instanceName = manager.getInstanceName()
        # String
        partition = self._message.getPartitionName()
        # String
        resourceName = self._message.getResourceName()
        # CurrentState
        currentStateDelta = CurrentState(resourceName)
        # StateTransitionError
        error = StateTransitionError(type, code, e)
        self._stateModel.rollbackOnError(self._message, self._notificationContext, error)
        if code == ErrorCode.ERROR: 
            currentStateDelta.setState(partition, "ERROR")
            self._stateModel.updateState("ERROR")
            accessor.updateProperty(keyBuilder.currentState(instanceName, self._message.getTgtSessionId(), resourceName), currentStateDelta)



    def onTimeout(self):
        """
        Returns void
        @Override


        """
        _isTimeout = True




