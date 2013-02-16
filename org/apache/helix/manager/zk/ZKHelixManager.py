# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from org.apache.helix.HelixConstants import ChangeType

#from org.apache.helix.HelixConstants.ChangeType import CONFIG
#from org.apache.helix.HelixConstants.ChangeType import CURRENT_STATE
#from org.apache.helix.HelixConstants.ChangeType import EXTERNAL_VIEW
#from org.apache.helix.HelixConstants.ChangeType import HEALTH
#from org.apache.helix.HelixConstants.ChangeType import IDEAL_STATE
#from org.apache.helix.HelixConstants.ChangeType import LIVE_INSTANCE
#from org.apache.helix.HelixConstants.ChangeType import MESSAGE

#from org.apache.helix.HelixConstants.ChangeType import MESSAGES_CONTROLLER
#from java.lang.management import ManagementFactory
#from java.net import InetAddress
#from java.net import UnknownHostException
#from java.util import ArrayList
#from java.util import Arrays
#from java.util import Iterator
#from java.util import LinkedList
#from java.util import List
#from java.util import Timer
#from java.util.concurrent import TimeUnit
#from org.I0Itec.zkclient import ZkConnection
#from org.apache.log4j import Logger
#from org.apache.zookeeper.Watcher.Event import EventType
#from org.apache.zookeeper.Watcher.Event import KeeperState
import time
from kazoo.protocol.states import KazooState

from org.apache.helix.HelixConstants import ZKEventType, KeeperState
from org.apache.helix.BaseDataAccessor import BaseDataAccessor
#from org.apache.helix.ClusterMessagingService import ClusterMessagingService
from org.apache.helix.ConfigAccessor import ConfigAccessor
#from org.apache.helix.ConfigChangeListener import ConfigChangeListener
from org.apache.helix.ConfigScope import ConfigScopeProperty
from org.apache.helix.ControllerChangeListener import ControllerChangeListener
from org.apache.helix.CurrentStateChangeListener import CurrentStateChangeListener
from org.apache.helix.DataAccessor import DataAccessor
from org.apache.helix.ExternalViewChangeListener import ExternalViewChangeListener
from org.apache.helix.HealthStateChangeListener import HealthStateChangeListener
#from org.apache.helix.HelixAdmin import HelixAdmin
from org.apache.helix.HelixConstants import ChangeType
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixManager import HelixManager
#from org.apache.helix.HelixTimerTask import HelixTimerTask
from org.apache.helix.IdealStateChangeListener import IdealStateChangeListener
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.LiveInstanceChangeListener import LiveInstanceChangeListener
from org.apache.helix.MessageListener import MessageListener
from org.apache.helix.PreConnectCallback import PreConnectCallback
#from org.apache.helix.PropertyKey import Builder
from org.apache.helix.PropertyPathConfig import PropertyPathConfig
from org.apache.helix.PropertyType import PropertyType
from org.apache.helix.ZNRecord import ZNRecord
#from org.apache.helix.controller.restlet.ZKPropertyTransferServer import ZKPropertyTransferServer
#from org.apache.helix.healthcheck.HealthStatsAggregationTask import HealthStatsAggregationTask
#from org.apache.helix.healthcheck.ParticipantHealthReportCollector import ParticipantHealthReportCollector
#from org.apache.helix.healthcheck.ParticipantHealthReportCollectorImpl import ParticipantHealthReportCollectorImpl
from org.apache.helix.manager.zk.BasicZkSerializer import BasicZkSerializer
from org.apache.helix.manager.zk.ByteArraySerializer import ByteArraySerializer
from org.apache.helix.manager.zk.CallbackHandler import CallbackHandler
from org.apache.helix.manager.zk.ChainedPathZkSerializer import ChainedPathZkSerializer
from org.apache.helix.manager.zk.DefaultControllerMessageHandlerFactory import DefaultControllerMessageHandlerFactory
from org.apache.helix.manager.zk.DefaultParticipantErrorMessageHandlerFactory import DefaultParticipantErrorMessageHandlerFactory
#from org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory import DefaultSchedulerMessageHandlerFactory
#from org.apache.helix.manager.zk.ZKDataAccessor import ZKDataAccessor
#from org.apache.helix.manager.zk.ZKHelixAdmin import ZKHelixAdmin
from org.apache.helix.manager.zk.ZKHelixDataAccessor import ZKHelixDataAccessor
#from org.apache.helix.manager.zk.ZKUtil import ZKUtil
#from org.apache.helix.manager.zk.ZNRecordStreamingSerializer import ZNRecordStreamingSerializer
from org.apache.helix.manager.zk.ZNRecordStreamingSerializer import ZNRecordStreamingSerializer
from org.apache.helix.manager.zk.ZkBaseDataAccessor import ZkBaseDataAccessor
#from org.apache.helix.manager.zk.ZkCacheBaseDataAccessor import ZkCacheBaseDataAccessor
from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.messaging.DefaultMessagingService import DefaultMessagingService
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.LiveInstance import LiveInstance
from org.apache.helix.model.Message import MessageType
from org.apache.helix.model.StateModelDefinition import StateModelDefinition
#from org.apache.helix.monitoring.ZKPathDataDumpTask import ZKPathDataDumpTask
#from org.apache.helix.participant.DistClusterControllerElection import DistClusterControllerElection
from org.apache.helix.participant.HelixStateMachineEngine import HelixStateMachineEngine
from org.apache.helix.participant.StateMachineEngine import StateMachineEngine
from org.apache.helix.store.PropertyStore import PropertyStore
from org.apache.helix.store.ZNRecordJsonSerializer import ZNRecordJsonSerializer
#from org.apache.helix.store.zk.ZKPropertyStore import ZKPropertyStore
from org.apache.helix.store.zk.ZkHelixPropertyStore import ZkHelixPropertyStore
#from org.apache.helix.tools.PropertiesReader import PropertiesReader

from org.apache.helix.manager.zk.ZkStateChangeListener import ZkStateChangeListener
from org.apache.helix.util.logger import get_logger
from org.apache.helix.util import TimeUnit
import socket, threading, os

class ZKHelixManager(HelixManager):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """
    Java modifiers:
         private final static
    Type:
        int
    """
    RETRY_LIMIT = 3

    """
    Java modifiers:
         private final static
    Type:
        int
    """
    CONNECTIONTIMEOUT = 60 * 1000

    """
    Java modifiers:
         private final static
    Type:
        int
    """
    DEFAULT_SESSION_TIMEOUT = 30 * 1000


    """

    Parameters:
        String clusterName
        String instanceName
        InstanceType instanceType
        String zkConnectString
    Throws:
        Exception
    """
    def __init__(self, clusterName, instanceName, instanceType, zkConnectString):
        self.logger.info("Create a zk-based cluster manager. clusterName:" + clusterName + ", instanceName:" + instanceName + ", type:" + InstanceType.toString(instanceType) + ", zkSvr:" + zkConnectString)
#        print("Create a zk-based cluster manager. clusterName:" + clusterName + ", instanceName:" + instanceName + ", type:" + InstanceType.toString(instanceType) + ", zkSvr:" + zkConnectString)
        # int
        sessionTimeoutInt = -1
        sessionTimeoutInt = self.DEFAULT_SESSION_TIMEOUT

        #        try:
#            sessionTimeoutInt = int(env["zk.session.timeout", "" + self.DEFAULT_SESSION_TIMEOUT))
#        except ValueError, e:
#            self.logger.warn("Exception while parsing session timeout: " + System.getProperty("zk.session.timeout", "" + self.DEFAULT_SESSION_TIMEOUT))

        if sessionTimeoutInt > 0: 
            self._sessionTimeout = sessionTimeoutInt
        else:
            self._sessionTimeout = self.DEFAULT_SESSION_TIMEOUT

        if instanceName == None: 
#            try:
                instanceName = socket.gethostbyname(socket.gethostname()) + "-" + instanceType.toString()
#            except UnknownHostException, e:
#                self.logger.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e)
#                instanceName = "UNKNOWN"


        self._clusterName = clusterName
        self._instanceName = instanceName
        self._instanceType = instanceType
        self._zkConnectString = zkConnectString
        self._zkStateChangeListener = ZkStateChangeListener(self)
        self._timer = None
#        self._handlers = ArrayList<CallbackHandler>()
        self._handlers = []
        self._messagingService = DefaultMessagingService(self)
        # TODO: fix ther version
        #self._version = PropertiesReader("cluster-manager-version.properties").getProperty("clustermanager.version")
        self._version = '0.5.1'
        self._stateMachEngine = HelixStateMachineEngine(self)
#        self._controllerTimerTasks = ArrayList<HelixTimerTask>()
        self._controllerTimerTasks = []
        if self._instanceType == InstanceType.CONTROLLER:
            raise HelixException("ERROR. Cannot handle CONTROLLER type for now!")
#            self._controllerTimerTasks.add(HealthStatsAggregationTask(self))

        self._preConnectCallbacks = []


    def isInstanceSetup(self):
        """
        Returns boolean
        Java modifiers:
             private

        """
        if self._instanceType == InstanceType.PARTICIPANT or self._instanceType == InstanceType.CONTROLLER_PARTICIPANT: 
            # boolean
            isValid = self._zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.PARTICIPANT), self._instanceName)) \
                      and self._zkClient.exists(PropertyPathConfig.getPath(PropertyType.MESSAGES, self._clusterName, self._instanceName)) \
                      and self._zkClient.exists(PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, self._clusterName, self._instanceName)) \
                      and self._zkClient.exists(PropertyPathConfig.getPath(PropertyType.STATUSUPDATES, self._clusterName, self._instanceName)) \
                    and self._zkClient.exists(PropertyPathConfig.getPath(PropertyType.ERRORS, self._clusterName, self._instanceName))
            return isValid

        return True


    def addIdealStateChangeListener(self, listener):
        """
        Returns void
        Parameters:
            listener: IdealStateChangeListener
        @Override


        Throws: 
            Exception
        """
        self.logger.info("ClusterManager.addIdealStateChangeListener()")
        self.checkConnected()
        # String
        path = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, self._clusterName)
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeDataChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated], ChangeType.IDEAL_STATE)
        self.addListener(callbackHandler)


    def addLiveInstanceChangeListener(self, listener):
        """
        Returns void
        Parameters:
            listener: LiveInstanceChangeListener
        @Override


        Throws: 
            Exception
        """
        self.logger.info("ClusterManager.addLiveInstanceChangeListener()")
        self.checkConnected()
        # String
        path = self._helixAccessor.keyBuilder().liveInstances().getPath()
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeDataChanged, ZKEventType.NodeChildrenChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.LIVE_INSTANCE)
        self.addListener(callbackHandler)


    def addConfigChangeListener(self, listener):
        pass
#        """
#        Returns void
#        Parameters:
#            listener: ConfigChangeListener
#        @Override
#
#
#        """
#        self.logger.info("ClusterManager.addConfigChangeListener()")
#        self.checkConnected()
#        # String
#        path = PropertyPathConfig.getPath(PropertyType.CONFIGS, self._clusterName, ConfigScopeProperty.PARTICIPANT.toString())
#        # CallbackHandler
#        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeChildrenChanged ], ChangeType.CONFIG)
#        self.addListener(callbackHandler)


    def addMessageListener(self, listener, instanceName):
        """
        Returns void
        Parameters:
            listener: MessageListenerinstanceName: String
        @Override


        """
        self.logger.info("ClusterManager.addMessageListener() " + instanceName)
        self.checkConnected()
        # String
        path = self._helixAccessor.keyBuilder().messages(instanceName).getPath()
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeChildrenChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.MESSAGE)
        self.addListener(callbackHandler)


    def addControllerMessageListener(self, listener):
        """
        Returns void
        Parameters:
            listener: MessageListener


        """
        self.logger.info("ClusterManager.addControllerMessageListener()")
        self.checkConnected()
        # String
        path = self._helixAccessor.keyBuilder().controllerMessages().getPath()
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeChildrenChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.MESSAGES_CONTROLLER)
        self.addListener(callbackHandler)


    def addCurrentStateChangeListener(self, listener, instanceName, sessionId):
        """
        Returns void
        Parameters:
            listener: CurrentStateChangeListenerinstanceName: StringsessionId: String
        @Override


        """
        self.logger.info("ClusterManager.addCurrentStateChangeListener() " + instanceName + " " + sessionId)
        self.checkConnected()
        # String
        path = self._helixAccessor.keyBuilder().currentStates(instanceName, sessionId).getPath()
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeChildrenChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.CURRENT_STATE)
        self.addListener(callbackHandler)


    def addHealthStateChangeListener(self, listener, instanceName):
        """
        Returns void
        Parameters:
            listener: HealthStateChangeListenerinstanceName: String
        @Override


        """
        self.logger.info("ClusterManager.addHealthStateChangeListener()" + instanceName)
        self.checkConnected()
        # String
        path = self._helixAccessor.keyBuilder().healthReports(instanceName).getPath()
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeChildrenChanged, ZKEventType.NodeDataChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.HEALTH)
        self.addListener(callbackHandler)


    def addExternalViewChangeListener(self, listener):
        """
        Returns void
        Parameters:
            listener: ExternalViewChangeListener
        @Override


        """
        self.logger.info("ClusterManager.addExternalViewChangeListener()")
        self.checkConnected()
        # String
        path = self._helixAccessor.keyBuilder().externalViews().getPath()
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeDataChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.EXTERNAL_VIEW)
        self.addListener(callbackHandler)


    def getDataAccessor(self):
        """
        Returns DataAccessor
        @Override


        """
        self.checkConnected()
        return self._accessor


    def getHelixDataAccessor(self):
        """
        Returns HelixDataAccessor
        @Override


        """
        self.checkConnected()
        return self._helixAccessor


    def getConfigAccessor(self):
        """
        Returns ConfigAccessor
        @Override


        """
        self.checkConnected()
        return self._configAccessor


    def getClusterName(self):
        """
        Returns String
        @Override


        """
        return self._clusterName


    def getInstanceName(self):
        """
        Returns String
        @Override


        """
        return self._instanceName


    def connect(self):
        """
        Returns void
        @Override


        Throws: 
            Exception
        """
        self.logger.info("ClusterManager.connect()")
        if self._zkStateChangeListener.isConnected(): 
            self.logger.warn("Cluster manager " + self._clusterName + " " + self._instanceName + " already connected")
            return
        # temp
        self.createClient(self._zkConnectString)
        self._messagingService.onConnected()

#        try:
#            self.createClient(self._zkConnectString)
#            self._messagingService.onConnected()
#        except Exception, e:
#            self.logger.error(e)
#            self.disconnect()
#            raise e




    def disconnect(self):
        """
        Returns void
        @Override


        """
        if not self.isConnected():
            self.logger.warn("ClusterManager " + self._instanceName + " already disconnected")
            return

        self.logger.info("disconnect " + self._instanceName + "(" + self._instanceType + ") from " + self._clusterName)
        self._messagingService.getExecutor().shutDown()
        self.resetHandlers()
        self._helixAccessor.shutdown()
        if self._leaderElectionHandler != None:
            self._leaderElectionHandler.reset()

        if self._participantHealthCheckInfoCollector != None:
            self._participantHealthCheckInfoCollector.stop()

        if self._timer != None: 
            self._timer.cancel()
            self._timer = None

        if self._instanceType == InstanceType.CONTROLLER: 
            self.stopTimerTasks()

        if self._propertyStore != None:
            self. _propertyStore.stop()

        self._zkClient.unsubscribeAll()
        self._zkClient.close()
        self._zkStateChangeListener.disconnect()
        self.logger.info("Cluster manager: " + self._instanceName + " disconnected")


    def getSessionId(self):
        """
        Returns String
        @Override


        """
        self.checkConnected()
        return self._sessionId


    def isConnected(self):
        """
        Returns boolean
        @Override


        """
        return self._zkStateChangeListener.isConnected()


    def getLastNotificationTime(self):
        """
        Returns long
        @Override


        """
        return -1


    def addControllerListener(self, listener):
        """
        Returns void
        Parameters:
            listener: ControllerChangeListener
        @Override


        """
        self.checkConnected()
        # String
        path = self._helixAccessor.keyBuilder().controller().getPath()
        self.logger.info("Add controller listener at: " + path)
        # CallbackHandler
        callbackHandler = self.createCallBackHandler(path, listener, [ZKEventType.NodeChildrenChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.CONTROLLER)
        self.addListener(callbackHandler)


    def removeListener(self, listener):
        """
        Returns boolean
        Parameters:
            listener: Object
        @Override


        """
        self.logger.info("remove listener: " + listener + " from " + self._instanceName)
#        synchronized (self) # Iterator<CallbackHandler>
#        iterator = self._handlers.iterator()
        newHandlers = []
        for handler in self._handlers:
            # CallbackHandler
#            handler = iterator.next()
            if (handler.getListener() == listener):
                handler.reset()
            else: newHandlers.append(handler)
        self._handlers = newHandlers

        return True


    def addLiveInstance(self):
        """
        Returns void
        Java modifiers:
             private

        """
        # LiveInstance
        liveInstance = LiveInstance(self._instanceName)
        liveInstance.setSessionId(self._sessionId)
        liveInstance.setHelixVersion(self._version)
        liveInstance.setLiveInstance("%s@%s" % (os.getpid(), socket.gethostname()))
#        liveInstance.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName())
        self.logger.info("Add live instance: InstanceName: " + self._instanceName + " Session id:" + str(self._sessionId))
        # Builder
        keyBuilder = self._helixAccessor.keyBuilder()
        if not self._helixAccessor.createProperty(keyBuilder.liveInstance(self._instanceName), liveInstance): 
            # String
            errorMsg = "Fail to create live instance node after waiting, so quit. instance:" + self._instanceName
            self.logger.warn(errorMsg)
            # TODO: still raise
#            raise HelixException(errorMsg)

        # String
        currentStatePathParent = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, self._clusterName, self._instanceName, self.getSessionId())
        if not self._zkClient.exists(currentStatePathParent): 
            self._zkClient.createPersistent(currentStatePathParent)
            self.logger.info("Creating current state path " + currentStatePathParent)



    def startStatusUpdatedumpTask(self):
        """
        Returns void
        Java modifiers:
             private

        """
        # long
        initialDelay = 30 * 60 * 1000
        # long
        period = 120 * 60 * 1000
        # int
        timeThresholdNoChange = 180 * 60 * 1000
        # TODO: comment out for now
#        if self._timer == None:
#            self._timer = Timer(True)
#            self._timer.scheduleAtFixedRate(ZKPathDataDumpTask(self, self._zkClient, timeThresholdNoChange), initialDelay, period)



    def createClient(self, zkServers):
        """
        Returns void
        Parameters:
            zkServers: String
        Java modifiers:
             private

        Throws: 
            Exception
        """
        # String
        propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, self._clusterName)
        # PathBasedZkSerializer
        zkSerializer = ChainedPathZkSerializer.builder(ZNRecordStreamingSerializer()).serialize(propertyStorePath, ByteArraySerializer()).build()
        # this does notwork, need ot use the path
#        zkSerializer = ByteArraySerializer()
#        zkSerializer = BasicZkSerializer(ByteArraySerializer())
        self._zkClient = ZkClient(zkServers, self._sessionTimeout, self.CONNECTIONTIMEOUT, zkSerializer)
        # dzhang: not there in latest version
#        self._accessor = ZKDataAccessor(self._clusterName, self._zkClient)
        # ZkBaseDataAccessor<ZNRecord>
#        baseDataAccessor = ZkBaseDataAccessor<ZNRecord>(self._zkClient)
        baseDataAccessor = ZkBaseDataAccessor(self._zkClient)
        if self._instanceType == InstanceType.PARTICIPANT:
            # String
            curStatePath = PropertyPathConfig.getPath(PropertyType.CURRENTSTATES, self._clusterName, self._instanceName)
            # TODO: handle cache later
            self._baseDataAccessor = baseDataAccessor
        #            self._baseDataAccessor = ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor, curStatePath)
        #            self._baseDataAccessor = ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor, Arrays.asList(curStatePath))
        else:
            if self._instanceType == InstanceType.CONTROLLER:
                # String
                extViewPath = PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, self._clusterName)
                # TODO: handle cache later
                self._baseDataAccessor = baseDataAccessor
#                self._baseDataAccessor = ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor, extViewPath)
#                self._baseDataAccessor = ZkCacheBaseDataAccessor<ZNRecord>(baseDataAccessor, Arrays.asList(extViewPath))
            else:
                self._baseDataAccessor = baseDataAccessor

        self._helixAccessor = ZKHelixDataAccessor(self._clusterName, self._instanceType, self._baseDataAccessor)
        self._configAccessor = ConfigAccessor(self._zkClient)
        # int
        retryCount = 0
        self._zkClient.subscribeStateChanges(self._zkStateChangeListener)
        # set the state?
        self._zkStateChangeListener.handleStateChanged(KazooState.CONNECTED)
        self._zkStateChangeListener.handleNewSession()

#        while retryCount < self.RETRY_LIMIT:
#            try:
#                # kazoo.client.start() will wait, should we call start here?
##                self._zkClient.waitUntilConnected(self._sessionTimeout, TimeUnit.MILLISECONDS)
##                self._zkStateChangeListener.handleStateChanged(KeeperState.SyncConnected)
#                self._zkStateChangeListener.handleStateChanged(KazooState.CONNECTED)
#                self._zkStateChangeListener.handleNewSession()
#                break
#            except HelixException, e:
#                self.logger.error("fail to createClient."+ str(e))
#                raise e
#
#            except Exception, e:
#                retryCount+=1
#                self.logger.error("fail to createClient. retry " + str(retryCount)+ str(e))
#                if retryCount == self.RETRY_LIMIT:
#                    raise e






    def createCallBackHandler(self, path, listener, eventTypes, changeType):
        """
        Returns CallbackHandler
        Parameters:
            path: Stringlistener: ObjecteventTypes: EventType[]changeType: ChangeType
        Java modifiers:
             private

        """
        if listener == None: 
            raise HelixException("Listener cannot be null")


        return CallbackHandler(self, self._zkClient, path, listener, eventTypes, changeType)


    def handleNewSession(self):
        """
        Returns void
        Java modifiers:
             protected

        """
        # boolean
        isConnected = self._zkClient.waitUntilConnected(self.CONNECTIONTIMEOUT, TimeUnit.MILLISECONDS)
        while not isConnected:
            self.logger.error("Could NOT connect to zk server in " + str(self.CONNECTIONTIMEOUT)+ "ms. zkServer: " + str(self._zkConnectString)+ ", expiredSessionId: " + str(self._sessionId)+ ", clusterName: " + str(self._clusterName))
            isConnected = self._zkClient.waitUntilConnected(self.CONNECTIONTIMEOUT, TimeUnit.MILLISECONDS)

        # ZkConnection
#        zkConnection = ((ZkConnection) self._zkClient.getConnection())
#        synchronized (self)
#        self._sessionId = Long.toHexString(zkConnection.getZookeeper().getSessionId())
        self._sessionId = self._zkClient.getSessionId()
#        self._accessor.reset()
        self._baseDataAccessor.reset()
        self.resetHandlers()
        self.logger.info("Handling new session, session id:" + str(self._sessionId) + ", instance:" + self._instanceName + ", instanceTye: " + InstanceType.toString(self._instanceType) + ", cluster: " + self._clusterName)
#        self.logger.info(zkConnection.getZookeeper())
        #TODO: enable this
#        if not ZKUtil.isClusterSetup(self._clusterName, self._zkClient):
#            raise HelixException("Initial cluster structure is not set up for cluster:" + self._clusterName)


        if not self.isInstanceSetup():
            raise HelixException("Initial cluster structure is not set up for instance:" + self._instanceName + " instanceType:" + InstanceType.toString(self._instanceType))


        if self._instanceType == InstanceType.PARTICIPANT or self._instanceType == InstanceType.CONTROLLER_PARTICIPANT: 
            self.handleNewSessionAsParticipant()

        if self._instanceType == InstanceType.CONTROLLER or self._instanceType == InstanceType.CONTROLLER_PARTICIPANT: 
            self.addControllerMessageListener(self._messagingService.getExecutor())

            # MessageHandlerFactory
            defaultControllerMsgHandlerFactory = DefaultControllerMessageHandlerFactory()
            self._messagingService.getExecutor().registerMessageHandlerFactory(defaultControllerMsgHandlerFactory.getMessageType(), defaultControllerMsgHandlerFactory)
            # TODO: skip MessageHandlerFactory for now
            # MessageHandlerFactory
#            defaultSchedulerMsgHandlerFactory = DefaultSchedulerMessageHandlerFactory(self)
#            self._messagingService.getExecutor().registerMessageHandlerFactory(defaultSchedulerMsgHandlerFactory.getMessageType(), defaultSchedulerMsgHandlerFactory)
            # MessageHandlerFactory
            defaultParticipantErrorMessageHandlerFactory = DefaultParticipantErrorMessageHandlerFactory(self)
            self._messagingService.getExecutor().registerMessageHandlerFactory(defaultParticipantErrorMessageHandlerFactory.getMessageType(), defaultParticipantErrorMessageHandlerFactory)
            # TODO: enable this
#            if self._leaderElectionHandler == None:
#                # String
#                path = PropertyPathConfig.getPath(PropertyType.CONTROLLER, self._clusterName)
#                self._leaderElectionHandler = self.createCallBackHandler(path, DistClusterControllerElection(self._zkConnectString), [ZKEventType.NodeChildrenChanged, ZKEventType.NodeDeleted, ZKEventType.NodeCreated ], ChangeType.CONTROLLER)
#            else:
#                self._leaderElectionHandler.init()


        if self._instanceType == InstanceType.PARTICIPANT or self._instanceType == InstanceType.CONTROLLER_PARTICIPANT or (self._instanceType == InstanceType.CONTROLLER and self.isLeader()):
            self.initHandlers()



    def handleNewSessionAsParticipant(self):
        """
        Returns void
        Java modifiers:
             private

        """
        # Builder
        keyBuilder = self._helixAccessor.keyBuilder()
        if self._helixAccessor.getProperty(keyBuilder.liveInstance(self._instanceName)) != None: 
            self.logger.warn("Found another instance with same instanceName: " + self._instanceName + " in cluster " + self._clusterName)
            try:
                time.sleep((self._sessionTimeout + 5000)/1000.0)
            except Exception, e:
                self.logger.warn("Sleep interrupted while waiting for previous liveinstance to go away.", e)

            if self._helixAccessor.getProperty(keyBuilder.liveInstance(self._instanceName)) != None: 
                # String
                errorMessage = "instance " + self._instanceName + " already has a liveinstance in cluster " + self._clusterName
                self.logger.error(errorMessage)
                raise HelixException(errorMessage)



        for callback in self._preConnectCallbacks:
            callback.onPreConnect()

        self.addLiveInstance()
        self.carryOverPreviousCurrentState()
        self._messagingService.registerMessageHandlerFactory(MessageType.toString(MessageType.STATE_TRANSITION), self._stateMachEngine)
        self.addMessageListener(self._messagingService.getExecutor(), self._instanceName)
        # TODO: do not add controller listener for now
#        self.addControllerListener(self._helixAccessor)
        # TODO: Do not do health report for now
#        if self._participantHealthCheckInfoCollector == None:
#            self._participantHealthCheckInfoCollector = ParticipantHealthReportCollectorImpl(self, self._instanceName)
#            self._participantHealthCheckInfoCollector.start()

        # String
        # TODO: Do not do health check for now
#        healthCheckInfoPath = self._helixAccessor.keyBuilder().healthReports(self._instanceName).getPath()
#        if not self._zkClient.exists(healthCheckInfoPath):
#            self._zkClient.createPersistent(healthCheckInfoPath, True)
#            self.logger.info("Creating healthcheck info path " + healthCheckInfoPath)



    def addPreConnectCallback(self, callback):
        """
        Returns void
        Parameters:
            callback: PreConnectCallback
        @Override


        """
        self.logger.info("Adding preconnect callback")
        self._preConnectCallbacks.add(callback)


    def resetHandlers(self):
        """
        Returns void
        Java modifiers:
             private

        """
#        synchronized (self)
#       # List<CallbackHandler>
        handlers = []
        handlers.extend(self._handlers)
        for handler in handlers:
            handler.reset()
            self.logger.info("reset handler: " + handler.getPath() + " by " + threading.currentThread())




    def initHandlers(self):
        """
        Returns void
        Java modifiers:
             private

        """
#        synchronized (self)
        # List<CallbackHandler>
#        handlers = []
#        handlers.addAll(self._handlers)
#        for handler in handlers: handler.init()
        for handler in self._handlers: handler.init()


    def addListener(self, handler):
        """
        Returns void
        Parameters:
            handler: CallbackHandler
        Java modifiers:
             private

        """
#        synchronized (self)
        self._handlers.append(handler)
        self.logger.info("add handler: " + handler.getPath() + " by " + str(threading.currentThread()))



    def isLeader(self):
        """
        Returns boolean
        @Override


        """
        if not self.isConnected():
            return False

        if self._instanceType != InstanceType.CONTROLLER: 
            return False

        # Builder
        keyBuilder = self._helixAccessor.keyBuilder()
        # LiveInstance
        leader = self._helixAccessor.getProperty(keyBuilder.controllerLeader())
        if leader == None: 
            return False
        else:
            # String
            leaderName = leader.getInstanceName()
            if leaderName == None or not (leaderName == self._instanceName): 
                return False


        return True


    def carryOverPreviousCurrentState(self):
        """
        Returns void
        Java modifiers:
             private

        """
        # Builder
        keyBuilder = self._helixAccessor.keyBuilder()
        # List<String>
        subPaths = self._helixAccessor.getChildNames(keyBuilder.sessions(self._instanceName))
        for previousSessionId in subPaths: # List<CurrentState>
            previousCurrentStates = self._helixAccessor.getChildValues(keyBuilder.currentStates(self._instanceName, previousSessionId))
            for previousCurrentState in previousCurrentStates:
                if not previousSessionId.upper() == self._sessionId.upper():
                    self.logger.info("Carrying over old session:" + previousSessionId + " resource " + previousCurrentState.getId() + " to new session:" + self._sessionId)
                    # String
                    stateModelDefRef = previousCurrentState.getStateModelDefRef()
                    if stateModelDefRef == None: 
                        self.logger.error("pervious current state doesn't have a state model def. skip it. prevCS: " + str(str(previousCurrentState)))
                        continue

                    # StateModelDefinition
                    stateModel = self._helixAccessor.getProperty(keyBuilder.stateModelDef(stateModelDefRef))
                    for partitionName in previousCurrentState.getPartitionStateMap().keys():
                        previousCurrentState.setState(partitionName, stateModel.getInitialState())

                    previousCurrentState.setSessionId(self._sessionId)
                    self._helixAccessor.setProperty(keyBuilder.currentState(self._instanceName, self._sessionId, previousCurrentState.getId()), previousCurrentState)



        for previousSessionId in subPaths:
            if not previousSessionId.upper() == self._sessionId.upper():
                # String
                path = self._helixAccessor.keyBuilder().currentStates(self._instanceName, previousSessionId).getPath()
                self.logger.info("Deleting previous current state. path: " + path + "/" + previousSessionId)
                self._zkClient.deleteRecursive(path)




    def getPropertyStore(self):
        """
        Returns PropertyStore<ZNRecord>
        @Deprecated
        @Override
        Java modifiers:
             synchronized

        """
        return None
        # TODO: skip Property Store for now
#        self.checkConnected()
#        if self._propertyStore == None:
#            # String
#            path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, self._clusterName)
#            self._propertyStore = ZKPropertyStore<ZNRecord>(self._zkClient, ZNRecordJsonSerializer(), path)
#
#        return self._propertyStore


    def getHelixPropertyStore(self):
        """
        Returns ZkHelixPropertyStore<ZNRecord>
        @Override
        Java modifiers:
             synchronized

        """
        self.checkConnected()
        if self._helixPropertyStore == None:
            # String
            path = PropertyPathConfig.getPath(PropertyType.HELIX_PROPERTYSTORE, self._clusterName)
            _helixPropertyStore = ZkHelixPropertyStore<ZNRecord>(ZkBaseDataAccessor<ZNRecord>(self._zkClient), path, None)

        return _helixPropertyStore


    def getClusterManagmentTool(self):
        """
        Returns HelixAdmin
        @Override
        Java modifiers:
             synchronized

        """
        raise HelixException("Not implemented")
#        self.checkConnected()
#        if self._zkClient != None:
#            _managementTool = ZKHelixAdmin(self._zkClient)
#        else:
#            self.logger.error("Couldn't get ZKClusterManagementTool because zkClient is null")
#
#        return _managementTool


    def getMessagingService(self):
        """
        Returns ClusterMessagingService
        @Override


        """
        return self._messagingService


    def getHealthReportCollector(self):
        """
        Returns ParticipantHealthReportCollector
        @Override


        """
        self.checkConnected()
        return self._participantHealthCheckInfoCollector


    def getInstanceType(self):
        """
        Returns InstanceType
        @Override


        """
        return self._instanceType


    def checkConnected(self):
        """
        Returns void
        Java modifiers:
             private

        """
        if not self.isConnected():
            raise HelixException("ClusterManager not connected. Call clusterManager.connect()")




    def getVersion(self):
        """
        Returns String
        @Override


        """
        return self._version


    def getStateMachineEngine(self):
        """
        Returns StateMachineEngine
        @Override


        """
        return self._stateMachEngine


    def getHandlers(self):
        """
        Returns List<CallbackHandler>
        Java modifiers:
             protected

        """
        return self._handlers


    def startTimerTasks(self):
        """
        Returns void
        @Override


        """
        for task in self._controllerTimerTasks: task.start()

        self.startStatusUpdatedumpTask()


    def stopTimerTasks(self):
        """
        Returns void
        @Override


        """
        for task in self._controllerTimerTasks: task.stop()




