# package org.apache.helix.participant
#from org.apache.helix.participant import *
#from java.lang.management import ManagementFactory
#from org.apache.log4j import Logger
from org.apache.helix.ControllerChangeListener import ControllerChangeListener
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.HelixManagerFactory import HelixManagerFactory
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.PropertyKey import Builder
from org.apache.helix.PropertyType import PropertyType
from org.apache.helix.controller.GenericHelixController import GenericHelixController
from org.apache.helix.controller.HelixControllerMain import HelixControllerMain
from org.apache.helix.controller.restlet.ZKPropertyTransferServer import ZKPropertyTransferServer
from org.apache.helix.model.LeaderHistory import LeaderHistory
from org.apache.helix.model.LiveInstance import LiveInstance


class DistClusterControllerElection(ControllerChangeListener):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(DistClusterControllerElection.class)







    """

    Parameters:
        String zkAddr
    """
    def __init__(self, zkAddr):
        self._zkAddr = zkAddr


    def onControllerChange(self, changeContext):
        """
        Returns void
        Parameters:
            changeContext: NotificationContext
        @Override
        Java modifiers:
             synchronized

        """
        # HelixManager
        manager = changeContext.getManager()
        if manager == None: 
            LOG.error("missing attributes in changeContext. requires HelixManager")
            return

        # InstanceType
        type = manager.getInstanceType()
        if type != InstanceType.CONTROLLER && type != InstanceType.CONTROLLER_PARTICIPANT: 
            LOG.error("fail to become controller because incorrect instanceType (was " + str(type.toString())+ ", requires CONTROLLER | CONTROLLER_PARTICIPANT)")
            return

        try:
            if (changeContext.getType() == NotificationContext.Type.INIT) or (changeContext.getType() == NotificationContext.Type.CALLBACK): 
                # HelixDataAccessor
                accessor = manager.getHelixDataAccessor()
                # Builder
                keyBuilder = accessor.keyBuilder()
                while (accessor.getProperty(keyBuilder.controllerLeader()) == None:
                    # boolean
                    success = tryUpdateController(manager)
                    if success: 
                        updateHistory(manager)
                        if type == InstanceType.CONTROLLER: 
                            HelixControllerMain.addListenersToController(manager, _controller)
                            manager.startTimerTasks()
                        else:
                            if type == InstanceType.CONTROLLER_PARTICIPANT: 
                                # String
                                clusterName = manager.getClusterName()
                                # String
                                controllerName = manager.getInstanceName()
                                _leader = HelixManagerFactory.getZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, _zkAddr)
                                _leader.connect()
                                _leader.startTimerTasks()
                                HelixControllerMain.addListenersToController(_leader, _controller)



            else:
                if (changeContext.getType() == NotificationContext.Type.FINALIZE): 
                    if _leader != None: 
                        _leader.disconnect()


        except Exception, e:
            LOG.error("Exception when trying to become leader"+ str(e))



    def tryUpdateController(self, manager):
        """
        Returns boolean
        Parameters:
            manager: HelixManager
        Java modifiers:
             private

        """
        # HelixDataAccessor
        accessor = manager.getHelixDataAccessor()
        # Builder
        keyBuilder = accessor.keyBuilder()
        # LiveInstance
        leader = LiveInstance(manager.getInstanceName())
        try:
            leader.setLiveInstance(ManagementFactory.getRuntimeMXBean().getName())
            leader.setSessionId(manager.getSessionId())
            leader.setHelixVersion(manager.getVersion())
            if ZKPropertyTransferServer.getInstance() != None: 
                # String
                zkPropertyTransferServiceUrl = ZKPropertyTransferServer.getInstance().getWebserviceUrl()
                if zkPropertyTransferServiceUrl != None: 
                    leader.setWebserviceUrl(zkPropertyTransferServiceUrl)

            else:
                LOG.warn("ZKPropertyTransferServer instnace is null")

            # boolean
            success = accessor.createProperty(keyBuilder.controllerLeader(), leader)
            if success: 
                return True
            else:
                LOG.info("Unable to become leader probably because some other controller becames the leader")

        except Exception, e:
            LOG.error("Exception when trying to updating leader record in cluster:" + str(manager.getClusterName())+ ". Need to check again whether leader node has been created or not"+ str(e))

        leader = accessor.getProperty(keyBuilder.controllerLeader())
        if leader != None: 
            # String
            leaderName = leader.getInstanceName()
            LOG.info("Leader exists for cluster:" + manager.getClusterName() + ", currentLeader:" + leaderName)
            if leaderName != None && (leaderName == manager.getInstanceName()): 
                return True


        return False


    def updateHistory(self, manager):
        """
        Returns void
        Parameters:
            manager: HelixManager
        Java modifiers:
             private

        """
        # HelixDataAccessor
        accessor = manager.getHelixDataAccessor()
        # Builder
        keyBuilder = accessor.keyBuilder()
        # LeaderHistory
        history = accessor.getProperty(keyBuilder.controllerLeaderHistory())
        if history == None: 
            history = LeaderHistory(PropertyType.HISTORY.toString())

        history.updateHistory(manager.getClusterName(), manager.getInstanceName())
        accessor.setProperty(keyBuilder.controllerLeaderHistory(), history)



