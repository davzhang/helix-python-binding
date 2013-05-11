# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
#from java.util import Date
#from java.util import Map
#from java.util import Timer
#from java.util import TimerTask
#from java.util.concurrent import Callable
#from org.apache.log4j import Logger
import time
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.PropertyKey import PropertyKey
#from org.apache.helix.PropertyKey import Builder
#from org.apache.helix.messaging.handling.GroupMessageHandler import GroupMessageInfo
from org.apache.helix.messaging.handling.HelixTaskResult import HelixTaskResult
from org.apache.helix.messaging.handling.MessageHandler import ErrorCode
from org.apache.helix.messaging.handling.MessageHandler import ErrorType
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageType
#from org.apache.helix.monitoring.StateTransitionContext import StateTransitionContext
#from org.apache.helix.monitoring.StateTransitionContext import StateTransitionContext
#from org.apache.helix.monitoring.StateTransitionDataPoint import StateTransitionDataPoint
#from org.apache.helix.util.StatusUpdateUtil import StatusUpdateUtil
from org.apache.helix.util.StatusUpdateUtil import StatusUpdateUtil
from org.apache.helix.util.logger import get_logger

#from org.apache.helix.util.UserExceptions import IllegalArgumentException
#from org.apache.helix.util.misc import enum

# call method
from org.apache.helix.util.misc import ternary

class HelixTask():
#class HelixTask(Callable<HelixTaskResult>):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

#    class TimeoutCancelTask(TimerTask):
#
#        """
#
#        Parameters:
#            HelixTaskExecutor executor
#            Message message
#            NotificationContext context
#        """
#        def __init__(self, executor, message, context):
#            self._executor = executor
#            self._message = message
#            self._context = context
#
#
#        def run(self):
#            """
#            Returns void
#            @Override
#
#
#            """
#            self._isTimeout = True
#            self.logger.warn("Message time out, canceling. id:" + self._message.getMsgId() + " timeout : " + self._message.getExecutionTimeout())
#            self._handler.onTimeout()
#            self._executor.cancelTask(self._message, self._context)
#


    """

    Parameters:
        Message message
        NotificationContext notificationContext
        MessageHandler handler
        HelixTaskExecutor executor
    Throws:
        Exception
    """
    def __init__(self, message, notificationContext, handler, executor):
        self._notificationContext = notificationContext
        self._message = message
        self._handler = handler
        self._manager = notificationContext.getManager()
        self._statusUpdateUtil = StatusUpdateUtil()
        self._executor = executor
        self._isTimeout = False


    def call(self):
        """
        Returns HelixTaskResult
        @Override


        """
        # Timer
        # TODO: enable timer
        timer = None
#        if self._message.getExecutionTimeout() > 0:
#            timer = Timer(True)
#            timer.schedule(TimeoutCancelTask(self._executor, self._message, self._notificationContext), self._message.getExecutionTimeout())
#            self.logger.info("Message starts with timeout " + self._message.getExecutionTimeout() + " MsgId:" + self._message.getMsgId())
#        else:
#            self.logger.info("Message does not have timeout. MsgId:" + self._message.getMsgId() + "/" + self._message.getPartitionName())

        # HelixTaskResult
        taskResult = HelixTaskResult()
        # Exception
        exception = None
        # ErrorType
        type = ErrorType.INTERNAL
        # ErrorCode
        code = ErrorCode.ERROR
        # long
        start = time.time()
        self.logger.info("msg:" + str(self._message.getMsgId()) + " handling task begin, at: " + str(start))
        # HelixDataAccessor
        accessor = self._manager.getHelixDataAccessor()
        self._statusUpdateUtil.logInfo(self._message, HelixTask, "Message handling task begin execute", accessor)
        self._message.setExecuteStartTimeStamp(time.time())
        try:
            taskResult = self._handler.handleMessage()
            exception = taskResult.getException()
#        except Exception, e:
        except KeyboardInterrupt, e:
            self._statusUpdateUtil.logError(self._message, HelixTask, e, "State transition interrupted, timeout:" + str(self._isTimeout),  accessor)
            self.logger.info("Message " + self._message.getMsgId() + " is interrupted")
            taskResult.setInterrupted(True)
            taskResult.setException(e)
            exception = e
#        except Exception, e:
#            # String
#            errorMessage = "Exception while executing a message. " + e + " msgId: " + self._message.getMsgId() + " type: " + self._message.getMsgType()
#            self.logger.error(errorMessage+ str(e))
#            self._statusUpdateUtil.logError(self._message, HelixTask, e, errorMessage, accessor)
#            taskResult.setSuccess(False)
#            taskResult.setException(e)
#            taskResult.setMessage(e.getMessage())
#            exception = e

        if timer != None: 
            timer.cancel()

        if taskResult.isSucess(): 
            self._statusUpdateUtil.logInfo(self._message, self._handler.__class__, "Message handling task completed successfully", accessor)
            self.logger.info("Message " + self._message.getMsgId() + " completed.")
        else:
            if taskResult.isInterrupted(): 
                self.logger.info("Message " + self._message.getMsgId() + " is interrupted")
                code = ternary(self._isTimeout, ErrorCode.TIMEOUT, ErrorCode.CANCEL)
                if self._isTimeout: 
                    # int
                    retryCount = self._message.getRetryCount()
                    self.logger.info("Message timeout, retry count: " + retryCount + " MSGID:" + self._message.getMsgId())
                    self._statusUpdateUtil.logInfo(self._message, self._handler.__class__, "Message handling task timeout, retryCount:" + retryCount, accessor)
                    if retryCount > 0: 
                        self._message.setRetryCount(retryCount - 1)
                        self._executor.scheduleTask(self._message, self._handler, self._notificationContext)
                        return taskResult


            else:
                # String
                errorMsg = "Message execution failed. msgId: " + self._message.getMsgId() + taskResult.getMessage()
                if exception is not None:
                    errorMsg += exception

                self.logger.error(errorMsg+ str(exception))
                self._statusUpdateUtil.logError(self._message, self._handler.getClass(), errorMsg, accessor)

        try:
            if not self._message.getGroupMessageMode(): 
                self.removeMessageFromZk(accessor, self._message)
                self.reportMessageStat(self._manager, self._message, taskResult)
                self.sendReply(accessor, self._message, taskResult)
            else:
                # GroupMessageInfo
                info = self._executor._groupMsgHandler.onCompleteSubMessage(self._message)
                if info is not None:
                    # Map<PropertyKey, CurrentState>
                    curStateMap = info.merge()
                    for key in curStateMap.keys():
                        accessor.updateProperty(key, curStateMap.get(key))

                    self.removeMessageFromZk(accessor, self._message)
                    self.reportMessageStat(self._manager, self._message, taskResult)
                    self.sendReply(accessor, self._message, taskResult)


            self._executor.reportCompletion(self._message)
        # except KeyboardInterrupt, e:
        #TODO: should we print the error
        except Exception as e:
            # String
            errorMessage = "Exception after executing a message, msgId: " + self._message.getMsgId() + e
            self.logger.error(errorMessage+ str(e))
            self._statusUpdateUtil.logError(self._message, HelixTask, errorMessage, accessor)
            exception = e
            type = ErrorType.FRAMEWORK
            code = ErrorCode.ERROR
        finally:
                # long
                end = time.time()
#                end = System.currentTimeMillis()
                self.logger.info("msg:" + self._message.getMsgId() + " handling task completed, results:" + str(taskResult.isSucess()) + ", at: " + str(end) + ", took:" + str(end - start))
                if exception is not None:
                    self._handler.onError(exception, code, type)


        return taskResult


    def removeMessageFromZk(self, accessor, message):
        """
        Returns void
        Parameters:
            accessor: HelixDataAccessormessage: Message
        Java modifiers:
             private

        """
        # Builder
        keyBuilder = accessor.keyBuilder()
        if message.getTgtName().lower() == "controller":
            accessor.removeProperty(keyBuilder.controllerMessage(message.getMsgId()))
        else:
            accessor.removeProperty(keyBuilder.message(self._manager.getInstanceName(), message.getMsgId()))



    def sendReply(self, accessor, message, taskResult):
        """
        Returns void
        Parameters:
            accessor: HelixDataAccessormessage: MessagetaskResult: HelixTaskResult
        Java modifiers:
             private

        """
        if self._message.getCorrelationId() != None and not (message.getMsgType() == MessageType.TASK_REPLY.toString()):
            self.logger.info("Sending reply for message " + message.getCorrelationId())
            self._statusUpdateUtil.logInfo(message, HelixTask, "Sending reply", accessor)
            taskResult.getTaskResultMap().put("SUCCESS", "" + taskResult.isSucess())
            taskResult.getTaskResultMap().put("INTERRUPTED", "" + taskResult.isInterrupted())
            if not taskResult.isSucess(): 
                taskResult.getTaskResultMap().put("ERRORINFO", taskResult.getMessage())

            # Message
            replyMessage = Message.createReplyMessage(self._message, self._manager.getInstanceName(), taskResult.getTaskResultMap())
            replyMessage.setSrcInstanceType(self._manager.getInstanceType())
            if message.getSrcInstanceType() == InstanceType.PARTICIPANT: 
                # Builder
                keyBuilder = accessor.keyBuilder()
                accessor.setProperty(keyBuilder.message(message.getMsgSrc(), replyMessage.getMsgId()), replyMessage)
            else:
                if message.getSrcInstanceType() == InstanceType.CONTROLLER: 
                    # Builder
                    keyBuilder = accessor.keyBuilder()
                    accessor.setProperty(keyBuilder.controllerMessage(replyMessage.getMsgId()), replyMessage)

            self._statusUpdateUtil.logInfo(message, HelixTask, "1 msg replied to " + replyMessage.getTgtName(), accessor)



    def reportMessageStat(self, manager, message, taskResult):
        """
        Returns void
        Parameters:
            manager: HelixManagermessage: MessagetaskResult: HelixTaskResult
        Java modifiers:
             private

        """
        pass
#        if not (message.getMsgType() == MessageType.STATE_TRANSITION.toString()):
#            return
#
#        # long
##        now = Date().getTime()
#        now = time.time()
#        # long
#        msgReadTime = message.getReadTimeStamp()
#        # long
#        msgExecutionStartTime = message.getExecuteStartTimeStamp()
#        if msgReadTime != 0 and msgExecutionStartTime != 0:
#            # long
#            totalDelay = now - msgReadTime
#            # long
#            executionDelay = now - msgExecutionStartTime
#            if totalDelay > 0 and executionDelay > 0:
#                # String
#                fromState = message.getFromState()
#                # String
#                toState = message.getToState()
#                # String
#                transition = fromState + "--" + toState
#                # StateTransitionContext
#                cxt = StateTransitionContext(manager.getClusterName(), manager.getInstanceName(), message.getResourceName(), transition)
#                # StateTransitionDataPoint
#                data = StateTransitionDataPoint(totalDelay, executionDelay, taskResult.isSucess())
#                self._executor.getParticipantMonitor().reportTransitionStat(cxt, data)
#
#        else:
#            self.logger.warn("message read time and start execution time not recorded.")





