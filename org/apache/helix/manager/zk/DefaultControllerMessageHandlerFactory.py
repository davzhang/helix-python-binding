# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from org.apache.log4j import Logger
from org.apache.helix.HelixException import HelixException
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.messaging.handling.HelixTaskResult import HelixTaskResult
from org.apache.helix.messaging.handling.MessageHandler import MessageHandler
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageType

from org.apache.helix.util.logger import get_logger

class DefaultControllerMessageHandlerFactory(MessageHandlerFactory):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    _logger = get_logger(__name__)

    def createHandler(self, message, context):
        """
        Returns MessageHandler
        Parameters:
            message: Messagecontext: NotificationContext
        @Override


        """
        # String
        type = message.getMsgType()
        if not (type == self.getMessageType()):
            raise HelixException("Unexpected msg type for message " + message.getMsgId() + " type:" + message.getMsgType())


        return self.DefaultControllerMessageHandler(message, context)


    def getMessageType(self):
        """
        Returns String
        @Override


        """
        return MessageType.CONTROLLER_MSG.toString()


    def reset(self):
        """
        Returns void
        @Override


        """


    class DefaultControllerMessageHandler(MessageHandler):

        """

        Parameters:
            Message message
            NotificationContext context
        """
        def __init__(self, message, context):
            super(message, context)


        def handleMessage(self):
            """
            Returns HelixTaskResult
            @Override


            Throws: 
                InterruptedException
            """
            # String
            type = self._message.getMsgType()
            # HelixTaskResult
            result = HelixTaskResult()
            if not (type == MessageType.CONTROLLER_MSG.toString()): 
                raise HelixException("Unexpected msg type for message " + self._message.getMsgId() + " type:" + self._message.getMsgType())


            result.getTaskResultMap().put("ControllerResult", "msg " + self._message.getMsgId() + " from " + self._message.getMsgSrc() + " processed")
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




