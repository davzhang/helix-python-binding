# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util import Arrays
#from org.apache.log4j import Logger
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.messaging.handling.HelixTaskResult import HelixTaskResult
from org.apache.helix.messaging.handling.MessageHandler import MessageHandler
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.model.Message import Message

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum

ActionOnError=enum('DISABLE_PARTITION', 'DISABLE_RESOURCE', 'DISABLE_INSTANCE')

class DefaultParticipantErrorMessageHandlerFactory(MessageHandlerFactory):



    """
    Java modifiers:
         final static
    Type:
        String
    """
    ACTIONKEY = "ActionOnError"

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


    class DefaultParticipantErrorMessageHandler(MessageHandler):



        """

        Parameters:
            Message message
            NotificationContext context
            HelixManager manager
        """
        def __init__(self, message, context, manager):
            super(message, context)
            self._manager = manager


        def handleMessage(self):
            """
            Returns HelixTaskResult
            @Override


            Throws: 
                InterruptedException
            """
            # HelixTaskResult
            result = HelixTaskResult()
            result.setSuccess(True)
            try:
                # ActionOnError
                actionOnError = ActionOnError.valueOf(self._message.getRecord().getSimpleField(DefaultParticipantErrorMessageHandlerFactory.ACTIONKEY))
                if actionOnError == ActionOnError.DISABLE_INSTANCE: 
                    self._manager.getClusterManagmentTool().enableInstance(self._manager.getClusterName(), self._message.getMsgSrc(), False)
                    self._logger.info("Instance " + self._message.getMsgSrc() + " disabled")
                else:
                    if actionOnError == ActionOnError.DISABLE_PARTITION: 
                        self._manager.getClusterManagmentTool().enablePartition(False, self._manager.getClusterName(), self._message.getMsgSrc(), self._message.getResourceName(), self._message.getPartitionName())
                        self._logger.info("partition " + self._message.getPartitionName() + " disabled")
                    else:
                        if actionOnError == ActionOnError.DISABLE_RESOURCE: 
                            self._logger.info("resource " + self._message.getResourceName() + " disabled")

            except Exception, e:
                self._logger.error(""+ str(e))
                result.setSuccess(False)
                result.setException(e)

            return result


        def onError(self, e, code, type):
            """
            Returns void
            Parameters:
                e: Exceptioncode: ErrorCodetype: ErrorType
            @Override


            """
            self._logger.error("Message handling pipeline get an exception. MsgId:" + str(self._message.getMsgId())+ str(e))



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


        return self.DefaultParticipantErrorMessageHandler(message, context, self._manager)


    def getMessageType(self):
        """
        Returns String
        @Override


        """
        return Message.MessageType.PARTICIPANT_ERROR_REPORT.toString()


    def reset(self):
        """
        Returns void
        @Override


        """



