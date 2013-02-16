# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import StringReader
#from java.util import HashMap
#from java.util import Map
#from java.util import TreeMap
#from java.util import UUID
#from java.util.concurrent import ConcurrentHashMap
#from org.apache.log4j import Logger
#from org.codehaus.jackson.map import ObjectMapper
from org.apache.helix.Criteria import Criteria
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.PropertyKey import Builder
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.messaging.AsyncCallback import AsyncCallback
from org.apache.helix.messaging.handling.HelixTaskResult import HelixTaskResult
from org.apache.helix.messaging.handling.MessageHandler import MessageHandler
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageType
from org.apache.helix.model.StatusUpdate import StatusUpdate
from org.apache.helix.util.StatusUpdateUtil import StatusUpdateUtil


class DefaultSchedulerMessageHandlerFactory(MessageHandlerFactory):

    """
    Java modifiers:
         final static
    Type:
        String
    """
    WAIT_ALL = "WAIT_ALL"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    SCHEDULER_MSG_ID = "SchedulerMessageId"

    Java modifiers:
         static
    class SchedulerAsyncCallback(AsyncCallback):









        """

        Parameters:
            Message originalMessage
            HelixManager manager
        """
        def __init__(self, originalMessage, manager):
            self._originalMessage = originalMessage
            self._manager = manager


        def onTimeOut(self):
            """
            Returns void
            @Override


            """
            _logger.info("Scheduler msg timeout " + _originalMessage.getMsgId() + " timout with " + _timeout + " Ms")
            _statusUpdateUtil.logError(_originalMessage, SchedulerAsyncCallback.class, "Task timeout", _manager.getHelixDataAccessor())
            addSummary(_resultSummaryMap, _originalMessage, _manager, True)


        def onReplyMessage(self, message):
            """
            Returns void
            Parameters:
                message: Message
            @Override


            """
            _logger.info("Update for scheduler msg " + _originalMessage.getMsgId() + " Message " + message.getMsgSrc() + " id " + message.getCorrelationId() + " completed")
            # String
            key = "MessageResult " + message.getMsgSrc() + " " + UUID.randomUUID()
            _resultSummaryMap.put(key, message.getResultMap())
            if self.isDone(): 
                _logger.info("Scheduler msg " + _originalMessage.getMsgId() + " completed")
                _statusUpdateUtil.logInfo(_originalMessage, SchedulerAsyncCallback.class, "Scheduler task completed", _manager.getHelixDataAccessor())
                addSummary(_resultSummaryMap, _originalMessage, _manager, False)



        def addSummary(self, _resultSummaryMap, originalMessage, manager, timeOut):
            """
            Returns void
            Parameters:
                _resultSummaryMap: Map<String, Map<String, String>>originalMessage: Messagemanager: HelixManagertimeOut: boolean
            Java modifiers:
                 private

            """
            # Map<String, String>
            summary = TreeMap<String, String>()
            summary.put("TotalMessages:", "" + _resultSummaryMap.size())
            summary.put("Timeout", "" + timeOut)
            _resultSummaryMap.put("Summary", summary)
            # HelixDataAccessor
            accessor = manager.getHelixDataAccessor()
            # Builder
            keyBuilder = accessor.keyBuilder()
            # ZNRecord
            statusUpdate = accessor.getProperty(keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), originalMessage.getMsgId())).getRecord()
            statusUpdate.getMapFields().putAll(_resultSummaryMap)
            accessor.setProperty(keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), originalMessage.getMsgId()), StatusUpdate(statusUpdate))



    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    _logger = Logger.getLogger(DefaultSchedulerMessageHandlerFactory.class)



    """

    Parameters:
        HelixManager manager
    """
    def __init__(self, manager):
        self._manager = manager


    def createHandler(self, message, context):
        """
        Returns MessageHandler
        Parameters:
            message: Messagecontext: NotificationContext
        @Override


        """
        # String
        type = message.getMsgType()
        if not (type == getMessageType()): 
            raise HelixException("Unexpected msg type for message " + message.getMsgId() + " type:" + message.getMsgType())


        return DefaultSchedulerMessageHandler(message, context, _manager)


    def getMessageType(self):
        """
        Returns String
        @Override


        """
        return MessageType.SCHEDULER_MSG.toString()


    def reset(self):
        """
        Returns void
        @Override


        """


    Java modifiers:
         static
    class DefaultSchedulerMessageHandler(MessageHandler):



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
            # String
            type = _message.getMsgType()
            # HelixTaskResult
            result = HelixTaskResult()
            if not (type == MessageType.SCHEDULER_MSG.toString()): 
                raise HelixException("Unexpected msg type for message " + _message.getMsgId() + " type:" + _message.getMsgType())


            # int
            timeOut = -1
            if _message.getRecord().getSimpleFields().containsKey("TIMEOUT"): 
                try:
                    timeOut = Integer.parseInt(_message.getRecord().getSimpleFields().get("TIMEOUT"))
                except Exception, e:


            # ZNRecord
            record = ZNRecord("templateMessage")
            record.getSimpleFields().putAll(_message.getRecord().getMapField("MessageTemplate"))
            # Message
            messageTemplate = Message(record)
            # StringReader
            sr = StringReader(_message.getRecord().getSimpleField("Criteria"))
            # ObjectMapper
            mapper = ObjectMapper()
            # Criteria
            recipientCriteria = None
            
            try:
                recipientCriteria = mapper.readValue(sr, Criteria.class)
            except Exception, e:
                _logger.error(""+ str(e))
                result.setException(e)
                result.setSuccess(False)
                return result

            _logger.info("Scheduler sending message, criteria:" + recipientCriteria)
            # boolean
            waitAll = False
            if _message.getRecord().getSimpleField(DefaultSchedulerMessageHandlerFactory.WAIT_ALL) != None: 
                try:
                    waitAll = Boolean.parseBoolean(_message.getRecord().getSimpleField(DefaultSchedulerMessageHandlerFactory.WAIT_ALL))
                except Exception, e:
                    _logger.warn("", e)


            # int
            nMsgsSent = 0
            # SchedulerAsyncCallback
            callback = SchedulerAsyncCallback(_message, _manager)
            if waitAll: 
                nMsgsSent = _manager.getMessagingService().sendAndWait(recipientCriteria, messageTemplate, callback, timeOut)
            else:
                nMsgsSent = _manager.getMessagingService().send(recipientCriteria, messageTemplate, callback, timeOut)

            # HelixDataAccessor
            accessor = _manager.getHelixDataAccessor()
            # Builder
            keyBuilder = accessor.keyBuilder()
            # Map<String, String>
            sendSummary = HashMap<String, String>()
            sendSummary.put("MessageCount", "" + nMsgsSent)
            # ZNRecord
            statusUpdate = accessor.getProperty(keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), _message.getMsgId())).getRecord()
            statusUpdate.getMapFields().put("SentMessageCount", sendSummary)
            accessor.setProperty(keyBuilder.controllerTaskStatus(MessageType.SCHEDULER_MSG.toString(), _message.getMsgId()), StatusUpdate(statusUpdate))
            result.getTaskResultMap().put("ControllerResult", "msg " + _message.getMsgId() + " from " + _message.getMsgSrc() + " processed")
            result.getTaskResultMap().put(SCHEDULER_MSG_ID, _message.getMsgId())
            result.setSuccess(True)
            return result


        def onError(self, e, code, type):
            """
            Returns void
            Parameters:
                e: Exceptioncode: ErrorCodetype: ErrorType
            @Override


            """
            _logger.error("Message handling pipeline get an exception. MsgId:" + str(_message.getMsgId())+ str(e))




