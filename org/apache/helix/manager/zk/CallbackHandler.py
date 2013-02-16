# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from org.apache.helix.HelixConstants import ChangeType
#from java.util import List
#from java.util.concurrent.atomic import AtomicLong
#from org.I0Itec.zkclient import IZkChildListener
#from org.I0Itec.zkclient import IZkDataListener
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger
#from org.apache.zookeeper.Watcher.Event import EventType
from org.apache.helix.ConfigChangeListener import ConfigChangeListener
from org.apache.helix.ControllerChangeListener import ControllerChangeListener
from org.apache.helix.CurrentStateChangeListener import CurrentStateChangeListener
from org.apache.helix.ExternalViewChangeListener import ExternalViewChangeListener
from org.apache.helix.HealthStateChangeListener import HealthStateChangeListener
from org.apache.helix.HelixConstants import ChangeType
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.IdealStateChangeListener import IdealStateChangeListener
from org.apache.helix.LiveInstanceChangeListener import LiveInstanceChangeListener
from org.apache.helix.MessageListener import MessageListener
from org.apache.helix.NotificationContext import NotificationContext
#from org.apache.helix.PropertyKey import Builder
from org.apache.helix.PropertyPathConfig import PropertyPathConfig
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.ExternalView import ExternalView
#from org.apache.helix.model.HealthStat import HealthStat
from org.apache.helix.model.IdealState import IdealState
from org.apache.helix.model.InstanceConfig import InstanceConfig
from org.apache.helix.model.LiveInstance import LiveInstance
from org.apache.helix.model.Message import Message

from org.apache.helix.manager.zk.ZKExceptionHandler import ZKExceptionHandler

from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.exceptions import NoNodeException, NodeExistsException
from org.apache.helix.util.logger import get_logger
#from kazoo.recipe.watchers import DataWatch
#from kazoo.recipe.watchers import ChildrenWatch
#from kazoo.exceptions import NodeExistsError
from org.apache.helix.util.logger import get_logger

import time, threading

#class CallbackHandler(IZkChildListener, IZkDataListener):

class CallbackHandler():

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)


    """

    Parameters:
        HelixManager manager
        ZkClient client
        String path
        Object listener
        EventType[] eventTypes
        ChangeType changeType
    """
    def __init__(self, manager, client, path, listener, eventTypes, changeType):
        self._manager = manager
        self._accessor = manager.getHelixDataAccessor()
        self._zkClient = client._connection # we need to use the actual kazoo client
        self._path = path
        self._listener = listener
        self._eventTypes = eventTypes
        self._changeType = changeType
        self.lastNotificationTimeStamp = time.time()
        self._watcherSet = False   # use the set only one watcher. The kazoo watcher do not set it again.
        # TODO: add the lock
#        lastNotificationTimeStamp = AtomicLong(System.nanoTime())
        self.init()


    def getListener(self):
        """
        Returns Object


        """
        return self._listener


    def getPath(self):
        """
        Returns String


        """
        return self._path


    def invoke(self, changeContext):
        """
        Returns void
        Parameters:
            changeContext: NotificationContext


        Throws: 
            Exception
        """
#        synchronized (self._manager) # Builder
        keyBuilder = self._accessor.keyBuilder()
        # long
        start = time.time()
#            start = System.currentTimeMillis()
#            if self.logger.isInfoEnabled():
        self.logger.info(str(threading.current_thread()) + " START:INVOKE " + self._path + " listener:" + self._listener.__class__.__name__)

        if self._changeType == ChangeType.IDEAL_STATE:
            # IdealStateChangeListener
            idealStateChangeListener = self._listener
#            idealStateChangeListener = (IdealStateChangeListener) self._listener
            self.subscribeForChanges(changeContext, self._path, True, True)
            # List<IdealState>
            idealStates = self._accessor.getChildValues(keyBuilder.idealStates())
            idealStateChangeListener.onIdealStateChange(idealStates, changeContext)
        else:
            if self._changeType == ChangeType.CONFIG:
                # ConfigChangeListener
                configChangeListener = self._listener
#                configChangeListener = (ConfigChangeListener) self._listener
                self.subscribeForChanges(changeContext, self._path, True, True)
                # List<InstanceConfig>
                configs = self._accessor.getChildValues(keyBuilder.instanceConfigs())
                configChangeListener.onConfigChange(configs, changeContext)
            else:
                if self._changeType == ChangeType.LIVE_INSTANCE:
                    # LiveInstanceChangeListener
                    liveInstanceChangeListener = self._listener
#                    liveInstanceChangeListener = (LiveInstanceChangeListener) self._listener
                    self.subscribeForChanges(changeContext, self._path, True, True)
                    # List<LiveInstance>
                    liveInstances = self._accessor.getChildValues(keyBuilder.liveInstances())
                    liveInstanceChangeListener.onLiveInstanceChange(liveInstances, changeContext)
                else:
                    if self._changeType == ChangeType.CURRENT_STATE:
                        # CurrentStateChangeListener
                        currentStateChangeListener = None
                        currentStateChangeListener = self._listener
#                        currentStateChangeListener = (CurrentStateChangeListener) self._listener
                        self.subscribeForChanges(changeContext, self._path, True, True)
                        # String
                        instanceName = PropertyPathConfig.getInstanceNameFromPath(self._path)
                        # String[]
                        pathParts = self._path.split("/")
                        # List<CurrentState>
                        currentStates = self._accessor.getChildValues(keyBuilder.currentStates(instanceName, pathParts[pathParts.length - 1]))
                        currentStateChangeListener.onStateChange(instanceName, currentStates, changeContext)
                    else:
                        if self._changeType == ChangeType.MESSAGE:
                            # MessageListener
                            messageListener = self._listener
#                            messageListener = (MessageListener) self._listener
                            self.subscribeForChanges(changeContext, self._path, True, False)
                            # String
                            instanceName = PropertyPathConfig.getInstanceNameFromPath(self._path)
                            # List<Message>
                            messages = self._accessor.getChildValues(keyBuilder.messages(instanceName))
                            messageListener.onMessage(instanceName, messages, changeContext)
                        else:
                            if self._changeType == ChangeType.MESSAGES_CONTROLLER:
                                # MessageListener
                                messageListener = self._listener
#                                messageListener = (MessageListener) self._listener
                                self.subscribeForChanges(changeContext, self._path, True, False)
                                # List<Message>
                                messages = self._accessor.getChildValues(keyBuilder.controllerMessages())
                                messageListener.onMessage(self._manager.getInstanceName(), messages, changeContext)
                            else:
                                if self._changeType == ChangeType.EXTERNAL_VIEW:
                                    # ExternalViewChangeListener
                                    externalViewListener = self._listener
#                                    externalViewListener = (ExternalViewChangeListener) self._listener
                                    self.subscribeForChanges(changeContext, self._path, True, True)
                                    # List<ExternalView>
                                    externalViewList = self._accessor.getChildValues(keyBuilder.externalViews())
                                    externalViewListener.onExternalViewChange(externalViewList, changeContext)
                                else:
                                    if self._changeType == ChangeType.CONTROLLER:
                                        # ControllerChangeListener
                                        controllerChangelistener = self._listener
#                                        controllerChangelistener = (ControllerChangeListener) self._listener
                                        self.subscribeForChanges(changeContext, self._path, True, False)
                                        controllerChangelistener.onControllerChange(changeContext)
                                    else:
                                        if self._changeType == ChangeType.HEALTH:
                                            # HealthStateChangeListener
                                            healthStateChangeListener = self._listener
#                                            healthStateChangeListener = (HealthStateChangeListener) self._listener
                                            self.subscribeForChanges(changeContext, self._path, True, True)
                                            # String
                                            instanceName = PropertyPathConfig.getInstanceNameFromPath(self._path)
                                            # List<HealthStat>
                                            healthReportList = self._accessor.getChildValues(keyBuilder.healthReports(instanceName))
                                            healthStateChangeListener.onHealthChange(instanceName, healthReportList, changeContext)

        # long
#        end = System.currentTimeMillis()
        end = time.time()
#            if self.logger.isInfoEnabled():
        self.logger.info(str(threading.current_thread()) + " END:INVOKE " + self._path + " listener:" + self._listener.__class__.__name__ + " Took: " + str(end - start))




    def subscribeForChanges(self, context, path, watchParent, watchChild):
        """
        Returns void
        Parameters:
            context: NotificationContextpath: StringwatchParent: booleanwatchChild: boolean
        Java modifiers:
             private

        """
        # NotificationContext.Type
        if self._watcherSet: return
        else: self._watcherSet = True
        type = context.getType()
        if watchParent: 
            if type == NotificationContext.Type.INIT or type == NotificationContext.Type.CALLBACK: 
                self.logger.info(self._manager.getInstanceName() + " subscribe child change@" + path)

                ChildrenWatch(self._zkClient, path, self.handleChildChange)
#                self._zkClient.subscribeChildChanges(path, self)
            else:
                if watchParent and type == NotificationContext.Type.FINALIZE:
                    self.logger.info(self._manager.__name__ + " UNsubscribe child change@" + path)
                    ChildrenWatch(self._zkClient, path, None)
#                    self._zkClient.unsubscribeChildChanges(path, self)


        if watchChild: 
            try:
                # List<String>
                childNames = self._zkClient.getChildren(path)
                if childNames == None or childNames.size() == 0: 
                    return

                for childName in childNames: # String
                    childPath = path + "/" + childName
                    if type == NotificationContext.Type.INIT or type == NotificationContext.Type.CALLBACK: 
#                        self.logger.debug(self._manager.getInstanceName() + " subscribe data change@" + str(childPath))
#                        pass
                        DataWatch(self._zkClient, path, self.handleDataChange)
#                        self._zkClient.subscribeDataChanges(childPath, self)
                    else:
                        if type == NotificationContext.Type.FINALIZE: 
                            self.logger.info(self._manager.__name__ + " UNsubscribe data change@" + childPath)
                            DataWatch(self._zkClient, path, None)
#                            self._zkClient.unsubscribeDataChanges(childPath, self)

                    self.subscribeForChanges(context, childPath, watchParent, watchChild)

            except NodeExistsException, e:
                self.logger.warn("fail to subscribe data change@" + path)




    def getEventTypes(self):
        """
        Returns EventType[]


        """
        return self._eventTypes


    def init(self):
        """
        Returns void


        """
        self.updateNotificationTime(time.time())
#        try:
#            # NotificationContext
#            changeContext = NotificationContext(self._manager)
#            changeContext.setType(NotificationContext.Type.INIT)
#            self.invoke(changeContext)
#        except Exception, e:
#            ZKExceptionHandler.getInstance().handle(e)

        changeContext = NotificationContext(self._manager)
        changeContext.setType(NotificationContext.Type.INIT)
        self.invoke(changeContext)



    def handleDataChange(self, data):
        """
        """
        dataPath = self._path
        try:
            self.updateNotificationTime(time.time())
            if dataPath != None and dataPath.startsWith(self._path):
                # NotificationContext
                changeContext = NotificationContext(self._manager)
                changeContext.setType(NotificationContext.Type.CALLBACK)
                self.invoke(changeContext)

#        except Exception, e:
        # TODO: change back the exception
        except KeyboardInterrupt, e:
            ZKExceptionHandler.getInstance().handle(e)

#    def handleDataChange(self, dataPath, data):
#        """
#        Returns void
#        Parameters:
#            dataPath: Stringdata: Object
#        @Override
#
#
#        """
#        try:
#            self.updateNotificationTime(time.time())
#            if dataPath != None and dataPath.startsWith(self._path):
#                # NotificationContext
#                changeContext = NotificationContext(self._manager)
#                changeContext.setType(NotificationContext.Type.CALLBACK)
#                self.invoke(changeContext)
#
#        except Exception, e:
#            ZKExceptionHandler.getInstance().handle(e)



    def handleDataDeleted(self, dataPath):
        """
        Returns void
        Parameters:
            dataPath: String
        @Override


        """
        try:
            self.updateNotificationTime(time.time())
            if dataPath != None and dataPath.find(self._path)==0:
                # NotificationContext
                changeContext = NotificationContext(self._manager)
                changeContext.setType(NotificationContext.Type.CALLBACK)
                ChildrenWatch(self._zkClient, dataPath, None)
#                self._zkClient.unsubscribeChildChanges(dataPath, self)
                self.invoke(changeContext)

#        except Exception, e:
        # TODO: change back the exception
        except KeyboardInterrupt, e:
            ZKExceptionHandler.getInstance().handle(e)


    def handleChildChange(self, currentChilds):
        '''
          kazoo does not return the parent path
        '''
        # TODO:
        parentPath = self._path  # temp hack
        try:
            self.updateNotificationTime(time.time())
            if parentPath != None and parentPath.find(self._path)==0:
                # NotificationContext
                changeContext = NotificationContext(self._manager)
                changeContext.setType(NotificationContext.Type.CALLBACK)
                self.invoke(changeContext)

#        except Exception, e:
        # TODO: change back the exception
        except KeyboardInterrupt, e:
            ZKExceptionHandler.getInstance().handle(e)

#    def handleChildChange(self, parentPath, currentChilds):
#        """
#        Returns void
#        Parameters:
#            parentPath: StringcurrentChilds: List<String>
#        @Override
#
#
#        """
#        try:
#            self.updateNotificationTime(time.time())
#            if parentPath != None and parentPath.startsWith(self._path):
#                # NotificationContext
#                changeContext = NotificationContext(self._manager)
#                changeContext.setType(NotificationContext.Type.CALLBACK)
#                self.invoke(changeContext)
#
#        except Exception, e:
#            ZKExceptionHandler.getInstance().handle(e)



    def reset(self):
        """
        Returns void


        """
        try:
            # NotificationContext
            changeContext = NotificationContext(self._manager)
            changeContext.setType(NotificationContext.Type.FINALIZE)
            self.invoke(changeContext)
#        except Exception, e:
        # TODO: change back the exception
        except KeyboardInterrupt, e:
            ZKExceptionHandler.getInstance().handle(e)



    def updateNotificationTime(self, nanoTime):
        """
        Returns void
        Parameters:
            nanoTime: long
        Java modifiers:
             private

        """
        # long
#        l = lastNotificationTimeStamp.get()
        l = self.lastNotificationTimeStamp
        self.lastNotificationTimeStamp = nanoTime
#        while (nanoTime > l):
#            # boolean
#            b = lastNotificationTimeStamp.compareAndSet(l, nanoTime)
#            if b:
#                break
#            else:
#                l = lastNotificationTimeStamp.get()





