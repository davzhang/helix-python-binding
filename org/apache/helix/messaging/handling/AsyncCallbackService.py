# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
#from java.util.concurrent import ConcurrentHashMap
#from org.apache.log4j import Logger
from org.apache.helix.HelixException import HelixException
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.messaging.AsyncCallback import AsyncCallback
from org.apache.helix.messaging.handling.HelixTaskResult import HelixTaskResult
from org.apache.helix.messaging.handling.MessageHandler import MessageHandler
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageType

from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
#from org.apache.helix.util.misc import enum

class AsyncCallbackMessageHandler(MessageHandler):



    """

    Parameters:
        String correlationId
        Message message
        NotificationContext context
    """
    def __init__(self, correlationId, message, context):
        super(message, context)
        self._correlationId = correlationId


    def handleMessage(self):
        """
        Returns HelixTaskResult
        @Override


        Throws:
            InterruptedException
        """
        self.verifyMessage(self._message)
        # HelixTaskResult
        result = HelixTaskResult()
        # assert (_correlationId.equalsIgnoreCase(_message.getCorrelationId()))
        self._logger.info("invoking reply message " + self._message.getMsgId() + ", correlationid:" + self._correlationId)
        # AsyncCallback
        callback = self._callbackMap.get(self._correlationId)
        # TODO
        #synchronized (callback)
        callback.onReply(self._message)
        if callback.isDone():
            self._logger.info("Removing finished callback, correlationid:" + self._correlationId)
            self._callbackMap.remove(self._correlationId)


        result.setSuccess(True)
        return result


    def onError(self, e, code, type):
        """
        Returns void
        Parameters:
            e: Exceptioncode: ErrorCodetype: ErrorType
        @Override


        """
        self._logger.error("Message handling pipeline get an exception. MsgId:" + str(self._message.getMsgId())+ str(e))


class AsyncCallbackService(MessageHandlerFactory):



    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    _logger = get_logger(__name__)

    """

    """
    def __init__(self): pass


    def registerAsyncCallback(self, correlationId, callback):
        """
        Returns void
        Parameters:
            correlationId: Stringcallback: AsyncCallback


        """
        if self._callbackMap.containsKey(correlationId): 
            self._logger.warn("correlation id " + correlationId + " already registered")

        self._logger.info("registering correlation id " + correlationId)
        self._callbackMap.put(correlationId, callback)


    def verifyMessage(self, message):
        """
        Returns void
        Parameters:
            message: Message


        """
        if not message.getMsgType().toString().upper() == MessageType.TASK_REPLY.toString().upper():
            # String
            errorMsg = "Unexpected msg type for message " + message.getMsgId() + " type:" + message.getMsgType() + " Expected : " + MessageType.TASK_REPLY
            self._logger.error(errorMsg)
            raise HelixException(errorMsg)


        # String
        correlationId = message.getCorrelationId()
        if correlationId == None: 
            # String
            errorMsg = "Message " + message.getMsgId() + " does not have correlation id"
            self._logger.error(errorMsg)
            raise HelixException(errorMsg)


        if not self._callbackMap.containsKey(correlationId): 
            # String
            errorMsg = "Message " + message.getMsgId() + " does not have correponding callback. Probably timed out already. Correlation id: " + correlationId
            self._logger.error(errorMsg)
            raise HelixException(errorMsg)


        self._logger.info("Verified reply message " + message.getMsgId() + " correlation:" + correlationId)


    def createHandler(self, message, context):
        """
        Returns MessageHandler
        Parameters:
            message: Messagecontext: NotificationContext
        @Override


        """
        self.verifyMessage(message)
        return AsyncCallbackMessageHandler(message.getCorrelationId(), message, context)


    def getMessageType(self):
        """
        Returns String
        @Override


        """
        return MessageType.toString(MessageType.TASK_REPLY)


    def reset(self):
        """
        Returns void
        @Override


        """






