# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
#from java.io import StringWriter
#from java.util import ArrayList
#from java.util import Collections
#from java.util import Date
#from java.util import HashSet
#from java.util import List
#from java.util import Map
#from java.util import Set
#from java.util.concurrent import Callable
#from java.util.concurrent import ConcurrentHashMap
#from java.util.concurrent import ExecutorService
#from java.util.concurrent import Executors
#from java.util.concurrent import Future
#from java.util.concurrent import TimeUnit
#from org.apache.log4j import Logger
#from org.codehaus.jackson.map import ObjectMapper
#from org.codehaus.jackson.map import SerializationConfig
import time
from org.apache.helix.ConfigAccessor import ConfigAccessor
from org.apache.helix.ConfigScope import ConfigScope
from org.apache.helix.ConfigScopeBuilder import ConfigScopeBuilder
from org.apache.helix.HelixConstants import HelixConstants
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.MessageListener import MessageListener
from org.apache.helix.NotificationContext import NotificationContext
#from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.PropertyKey import PropertyKey
#from org.apache.helix.PropertyKey import Builder
#from org.apache.helix.PropertyType import Type
from org.apache.helix.messaging.handling.GroupMessageHandler import GroupMessageHandler
from org.apache.helix.messaging.handling.HelixTask import HelixTask
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import Attributes
from org.apache.helix.model.Message import MessageState
from org.apache.helix.model.Message import MessageType
#from org.apache.helix.monitoring.ParticipantMonitor import ParticipantMonitor
#from org.apache.helix.participant.HelixStateMachineEngine import HelixStateMachineEngine
from org.apache.helix.participant import HelixStateMachineEngine
from org.apache.helix.util import TimeUnit, Executors
from org.apache.helix.util.StatusUpdateUtil import StatusUpdateUtil

from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
#from org.apache.helix.util.misc import enum
import threading
from concurrent import futures

class HelixTaskExecutor(MessageListener):

    """
    Java modifiers:
         final static
    Type:
        int
    """
    DEFAULT_PARALLEL_TASKS = 40

    """
    Java modifiers:
         final static
    Type:
        String
    """
    MAX_THREADS = "maxThreads"


    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)


    """

    """
    def __init__(self):
        self._taskMap = {}
#        self._taskMap = ConcurrentHashMap<String, Future<HelixTaskResult>>()
        self._groupMsgHandler = GroupMessageHandler()
#        self._lock = Object()
        self._lock = threading.RLock()
        self._statusUpdateUtil = StatusUpdateUtil()
#        self._monitor = ParticipantMonitor()
        self._monitor = None
        self.startMonitorThread()
        self._handlerFactoryMap = {} # ConcurrentHashMap<String, MessageHandlerFactory>();
        self._threadpoolMap = {} # new ConcurrentHashMap<String, ExecutorService>();
        self._resourceThreadpoolSizeMap = {}  # ConcurrentHashMap<String, Integer>();


#    def registerMessageHandlerFactory(self, type, factory):
#        """
#        Returns void
#        Parameters:
#            type: Stringfactory: MessageHandlerFactory
#
#
#        """
#        self.registerMessageHandlerFactory(type, factory, HelixTaskExecutor.DEFAULT_PARALLEL_TASKS)
#

    def registerMessageHandlerFactory(self, type, factory, threadpoolSize=DEFAULT_PARALLEL_TASKS):
        """
        Returns void
        Parameters:
            type: Stringfactory: MessageHandlerFactorythreadpoolSize: int


        """
        if not self._handlerFactoryMap.__contains__(type):
            if not type.upper() == factory.getMessageType().upper():
                raise HelixException("Message factory type mismatch. Type: " + type + " factory : " + factory.getMessageType())


            self._handlerFactoryMap.__setitem__(type, factory)
            self._threadpoolMap.__setitem__(type, futures.ThreadPoolExecutor(threadpoolSize))
#            self._threadpoolMap.__setitem__(type, Executors.newFixedThreadPool(threadpoolSize))
            self.LOG.info("Adding msg factory for type " + type + " threadpool size " + str(threadpoolSize))
        else:
            self.LOG.error("Ignoring duplicate msg handler factory for type " + str(type))



    def getParticipantMonitor(self):
        """
        Returns ParticipantMonitor


        """
        return self._monitor


    def startMonitorThread(self):
        """
        Returns void
        Java modifiers:
             private

        """


    def checkResourceConfig(self, resourceName, manager):
        """
        Returns void
        Parameters:
            resourceName: Stringmanager: HelixManager


        """
        if not self._resourceThreadpoolSizeMap.__contains__(resourceName):
            # int
            threadpoolSize = -1
            # ConfigAccessor
            configAccessor = manager.getConfigAccessor()
            if configAccessor != None: 
                # ConfigScope
                scope = ConfigScopeBuilder().forCluster(manager.getClusterName()).forResource(resourceName).build()
                # String
                threadpoolSizeStr = configAccessor.get(scope, HelixTaskExecutor.MAX_THREADS)
                try:
                    if threadpoolSizeStr != None: 
                        threadpoolSize = int(threadpoolSizeStr)

                except Exception, e:
                    self.LOG.error(""+ str(e))


            if threadpoolSize > 0: 
                # String
                key = MessageType.toString(MessageType.STATE_TRANSITION) + "." + resourceName
                self._threadpoolMap[key] = futures.ThreadPoolExecutor(threadpoolSize)
#                self._threadpoolMap.[key] = Executors.newFixedThreadPool(threadpoolSize))
                self.LOG.info("Adding per resource threadpool for resource " + resourceName + " with size " + str(threadpoolSize))

            self._resourceThreadpoolSizeMap.__setitem__(resourceName, threadpoolSize)



    def findExecutorServiceForMsg(self, message):
        """
        Returns ExecutorService
        Parameters:
            message: Message


        """
        # ExecutorService
        executorService = self._threadpoolMap.get(message.getMsgType())
        if (message.getMsgType() == MessageType.toString(MessageType.STATE_TRANSITION)):
            # String
            resourceName = message.getResourceName()
            if resourceName != None: 
                # String
                key = message.getMsgType() + "." + resourceName
                if self._threadpoolMap.__contains__(key):
                    self.LOG.info("Find per-resource thread pool with key " + key)
                    executorService = self._threadpoolMap.get(key)



        return executorService


    def scheduleTask(self, message, handler, notificationContext):
        """
        Returns void
        Parameters:
            message: Messagehandler: MessageHandlernotificationContext: NotificationContext


        """
        # assert (handler != None)
#        synchronized (self._lock)
        self._lock.acquire()
        try:
            # String
            taskId = message.getMsgId() + "/" + message.getPartitionName()
            if message.getMsgType() == MessageType.toString(MessageType.STATE_TRANSITION):
                self.checkResourceConfig(message.getResourceName(), notificationContext.getManager())

            self.LOG.info("Scheduling message: " + taskId)
            self._statusUpdateUtil.logInfo(message, HelixTaskExecutor, "Message handling task scheduled", notificationContext.getManager().getHelixDataAccessor())
            # HelixTask
            task = HelixTask(message, notificationContext, handler, self)
            if not self._taskMap.__contains__(taskId):
                self.LOG.info("Message:" + taskId + " handling task scheduled")
                # Future<HelixTaskResult>
                future = self.findExecutorServiceForMsg(message).submit(task.call)
                self._taskMap.__setitem__(taskId, future)
            else:
#                pass
                self._statusUpdateUtil.logWarning(message, HelixTaskExecutor, "Message handling task already sheduled for " + taskId, notificationContext.getManager().getHelixDataAccessor())

        except KeyboardInterrupt, e:
        # TODO. enable catch
#        except Exception, e:
            self.LOG.error("Error while executing task." + str(message)+ str(e))
            self._statusUpdateUtil.logError(message, HelixTaskExecutor, e, "Error while executing task " + str(e), notificationContext.getManager().getHelixDataAccessor())
        finally:
            self._lock.release()




    def cancelTask(self, message, notificationContext):
        """
        Returns void
        Parameters:
            message: MessagenotificationContext: NotificationContext


        """
        #synchronized (self._lock)
        self._lock.acquire()
        try:
        # # String
            taskId = message.getMsgId() + "/" + message.getPartitionName()
            if self._taskMap.__contains__(taskId):
                self._statusUpdateUtil.logInfo(message, HelixTaskExecutor, "Trying to cancel the future for " + taskId, notificationContext.getManager().getHelixDataAccessor())
                # Future<HelixTaskResult>
                future = self._taskMap.get(taskId)
                if future.cancel(True): 
                    self._statusUpdateUtil.logInfo(message, HelixTaskExecutor, "Canceled " + taskId, notificationContext.getManager().getHelixDataAccessor())
                    self._taskMap.pop(taskId)
                else:
                    self._statusUpdateUtil.logInfo(message, HelixTaskExecutor, "false when trying to cancel the message " + taskId, notificationContext.getManager().getHelixDataAccessor())

            else:
                self._statusUpdateUtil.logWarning(message, HelixTaskExecutor, "Future not found when trying to cancel " + taskId, notificationContext.getManager().getHelixDataAccessor())
        finally:
            self._lock.release()




    def reportCompletion(self, message):
        """
        Returns void
        Parameters:
            message: Message
        Java modifiers:
             protected

        """
#        synchronized (self._lock) # String
        self._lock.acquire()
        try:
            taskId = message.getMsgId() + "/" + message.getPartitionName()
            self.LOG.info("message finished: " + taskId + ", took " + (time.time() - message.getExecuteStartTimeStamp()))
            if self._taskMap.__contains__(taskId):
                self._taskMap.pop(taskId)
            else:
                self.LOG.warn("message " + taskId + "not found in task map")
        finally:
            self._lock.release()




    def updateMessageState(self, readMsgs, accessor, instanceName):
        """
        Returns void
        Parameters:
            readMsgs: List<Message>accessor: HelixDataAccessorinstanceName: String
        Java modifiers:
             private

        """
        # Builder
        keyBuilder = accessor.keyBuilder()
        # List<PropertyKey>
        readMsgKeys = []
#        readMsgKeys = ArrayList<PropertyKey>()
        for msg in readMsgs:
            readMsgKeys.append(msg.getKey(keyBuilder, instanceName))

        accessor.setChildren(readMsgKeys, readMsgs)


    def onMessage(self, instanceName, messages, changeContext):
        """
        Returns void
        Parameters:
            instanceName: Stringmessages: List<Message>changeContext: NotificationContext
        @Override


        """
        if changeContext.getType() == NotificationContext.Type.FINALIZE:
            self.LOG.info("Get FINALIZE notification")
            for factory in self._handlerFactoryMap.values():
                factory.reset()
                for f in self._taskMap.values():# Future<HelixTaskResult>
                 f.cancel(True)
                self._taskMap.clear()

            return

        # HelixManager
        manager = changeContext.getManager()
        # HelixDataAccessor
        accessor = manager.getHelixDataAccessor()
        # Builder
        keyBuilder = accessor.keyBuilder()
        if messages == None or len(messages) == 0:
            self.LOG.info("No Messages to process")
            return

#        Collections.sort(messages, Message.CREATE_TIME_COMPARATOR)
        messages.sort(Message.CREATE_TIME_COMPARATOR)
        # List<MessageHandler>
        handlers = []
#        handlers = ArrayList<MessageHandler>()
        # List<Message>
#        readMsgs = ArrayList<Message>()
        readMsgs = []
        # String
        sessionId = manager.getSessionId()
        # List<String>
        curResourceNames = accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId))
        # List<PropertyKey>
        createCurStateKeys = []
        # List<CurrentState>
        metaCurStates = []
        # Set<String>
        createCurStateNames = set()
        changeContext.add(NotificationContext.TASK_EXECUTOR_KEY, self)
        for message in messages:
            if message.getMsgType().upper() == MessageType.toString(MessageType.NO_OP).upper():
                self.LOG.info("Dropping NO-OP message. mid: " + message.getId() + ", from: " + message.getMsgSrc())
                accessor.removeProperty(message.getKey(keyBuilder, instanceName))
                continue

            # String
            tgtSessionId = message.getTgtSessionId()
            if not (sessionId == tgtSessionId) and not (tgtSessionId == "*"):
                # String
                warningMessage = "SessionId does NOT match. expected sessionId: " + sessionId + ", tgtSessionId in message: " + tgtSessionId + ", messageId: " + message.getMsgId()
                self.LOG.warn(warningMessage)
                accessor.removeProperty(message.getKey(keyBuilder, instanceName))
                self._statusUpdateUtil.logWarning(message, HelixStateMachineEngine, warningMessage, accessor)
                continue

            if MessageState.NEW != message.getMsgState(): 
                self.LOG.trace("Message already read. mid: " + message.getMsgId())
                continue

            try:
                # List<MessageHandler>
                createHandlers = self.createMessageHandlers(message, changeContext)
#                if createHandlers.isEmpty():
                if not createHandlers:
                    continue

                handlers.extend(createHandlers)
            except KeyboardInterrupt, e:
            #TODO: enable this?
#            except Exception, e:
                # String
                error = "Failed to create message handler for " + message.getMsgId() + ", exception: " + str(e)
                self._statusUpdateUtil.logError(message, HelixStateMachineEngine, e, error, accessor)
                message.setMsgState(MessageState.UNPROCESSABLE)
                accessor.removeProperty(message.getKey(keyBuilder, instanceName))
                # TODO: comment out for now
#                # ObjectMapper
#                mapper = ObjectMapper()
#                # SerializationConfig
#                serializationConfig = mapper.getSerializationConfig()
#                serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, True)
#                # StringWriter
#                sw = StringWriter()
#                try:
#                    mapper.writeValue(sw, message.getRecord())
#                    self.LOG.error("Message cannot be processed:" + str(sw.toString())+ str(e))
#                except Exception, ex:
#                    self.LOG.error(""+ str(ex))

                continue

            message.setMsgState(MessageState.READ)
            message.setReadTimeStamp(time.time())
            message.setExecuteSessionId(changeContext.getManager().getSessionId())
            self._statusUpdateUtil.logInfo(message, HelixStateMachineEngine, "New Message", accessor)
            readMsgs.append(message)
            if not message.isControlerMsg() and (message.getMsgType() == MessageType.toString(MessageType.STATE_TRANSITION)):
                # String
                resourceName = message.getResourceName()
                if not curResourceNames.__contains__(resourceName) and not createCurStateNames.__contains__(resourceName):
                    createCurStateNames.add(resourceName)
                    createCurStateKeys.append(keyBuilder.currentState(instanceName, sessionId, resourceName))
                    # CurrentState
                    metaCurState = CurrentState(resourceName)
                    metaCurState.setBucketSize(message.getBucketSize())
                    metaCurState.setStateModelDefRef(message.getStateModelDef())
                    metaCurState.setSessionId(sessionId)
                    metaCurState.setGroupMessageMode(message.getGroupMessageMode())
                    # String
                    ftyName = message.getStateModelFactoryName()
                    if ftyName != None: 
                        metaCurState.setStateModelFactoryName(ftyName)
                    else:
                        metaCurState.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY)

                    metaCurStates.append(metaCurState)



        if createCurStateKeys.__len__() > 0:
            # TODO: enable the try/catch?
#            try:
            accessor.createChildren(createCurStateKeys, metaCurStates)
#            except Exception, e:
#                self.LOG.error(e)


        if readMsgs.__len__() > 0:
            self.updateMessageState(readMsgs, accessor, instanceName)
            for handler in handlers:
                self.scheduleTask(handler._message, handler, changeContext)




    def createMessageHandler(self, message, changeContext):
        """
        Returns MessageHandler
        Parameters:
            message: MessagechangeContext: NotificationContext
        Java modifiers:
             private

        """
        # String
        msgType = str(message.getMsgType())
        # MessageHandlerFactory
        handlerFactory = self._handlerFactoryMap.get(msgType)
        if handlerFactory == None: 
            self.LOG.warn("Fail to find message handler factory for type: " + msgType + " mid:" + message.getMsgId())
            return None

        return handlerFactory.createHandler(message, changeContext)


    def createMessageHandlers(self, message, changeContext):
        """
        Returns List<MessageHandler>
        Parameters:
            message: MessagechangeContext: NotificationContext
        Java modifiers:
             private

        """
        # List<MessageHandler>
        handlers = []
        if not message.getGroupMessageMode(): 
            self.LOG.info("Creating handler for message " + message.getMsgId() + "/" + message.getPartitionName())
            # MessageHandler
            handler = self.createMessageHandler(message, changeContext)
            if handler != None: 
                handlers.append(handler)

        else:
            self._groupMsgHandler.put(message)
            # List<String>
            partitionNames = message.getPartitionNames()
            for partitionName in partitionNames: # Message
                subMsg = Message(message.getRecord())
                subMsg.setPartitionName(partitionName)
                subMsg.setAttribute(Attributes.PARENT_MSG_ID, message.getId())
                self.LOG.info("Creating handler for group message " + subMsg.getMsgId() + "/" + partitionName)
                # MessageHandler
                handler = self.createMessageHandler(subMsg, changeContext)
                if handler != None: 
                    handlers.append(handler)



        return handlers


    def shutDown(self):
        """
        Returns void


        """
        self.LOG.info("shutting down TaskExecutor")
#        synchronized (self._lock)
        self._lock.acquire()
        try:
            for msgType in self._threadpoolMap.keys(): # List<Runnable>
                tasksLeft = self._threadpoolMap.get(msgType).shutdownNow()
                self.LOG.info(tasksLeft.size() + " tasks are still in the threadpool for msgType " + msgType)

            for msgType in self._threadpoolMap.keys():
                try:
                    if not self._threadpoolMap.get(msgType).awaitTermination(200, TimeUnit.MILLISECONDS): 
                        self.LOG.warn(msgType + " is not fully termimated in 200 MS")
                        print(msgType + " is not fully termimated in 200 MS")

#                except InterruptedException, e:
                except Exception, e:
                    self.LOG.error("Interrupted"+ str(e))
        finally:
            self._lock.release()


        self._monitor.shutDown()
        self.LOG.info("shutdown finished")


#    def main(args):
#        """
#        Returns void
#        Parameters:
#            args: String[]
#        Java modifiers:
#             static
#
#        Throws:
#            Exception
#        """
#        # ExecutorService
#        pool = Executors.newFixedThreadPool(DEFAULT_PARALLEL_TASKS)
#        # Future<HelixTaskResult>
#        future = None
#
#        future = pool.submit(Callable<HelixTaskResult>() {
#
#            def call(self):
#                """
#                Returns HelixTaskResult
#                @Override
#
#
#                Throws:
#                    Exception
#                """
#                System.out.println("CMTaskExecutor.main(...).new Callable() {...}.call()")
#                return None
#
#        })
#        future = pool.submit(HelixTask(None, None, None, None))
#        Thread.currentThread().join()
#        System.out.println(future.isDone())



