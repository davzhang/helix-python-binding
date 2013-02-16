# package org.apache.helix.participant
#from org.apache.helix.participant import *
#from java.util import List
#from org.apache.log4j import Logger
from org.apache.helix.HelixConstants import ChangeType
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelInfo import StateModelInfo
from org.apache.helix.participant.statemachine.Transition import Transition

# Annotation @StateModelInfo(initialState = "OFFLINE", states = { "LEADER", "STANDBY" })


class GenericLeaderStandbyModel(StateModel):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(GenericLeaderStandbyModel.class)





    """

    Parameters:
        CustomCodeCallbackHandler callback
        List<ChangeType> notificationTypes
        String partitionKey
    """
    def __init__(self, callback, notificationTypes, partitionKey):
        self._particHolder = CustomCodeInvoker(callback, partitionKey)
        self._notificationTypes = notificationTypes


    def onBecomeStandbyFromOffline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "STANDBY", from = "OFFLINE")



        """
        LOG.info("Become STANDBY from OFFLINE")


    def onBecomeLeaderFromStandby(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "LEADER", from = "STANDBY")



        Throws: 
            Exception
        """
        LOG.info("Become LEADER from STANDBY")
        # HelixManager
        manager = context.getManager()
        if manager == None: 
            raise IllegalArgumentException("Require HelixManager in notification conext")


        for # ChangeType
        notificationType = None
         in _notificationTypes) if notificationType == ChangeType.LIVE_INSTANCE: 
                manager.addLiveInstanceChangeListener(_particHolder)
            else:
                if notificationType == ChangeType.CONFIG: 
                    manager.addConfigChangeListener(_particHolder)
                else:
                    if notificationType == ChangeType.EXTERNAL_VIEW: 
                        manager.addExternalViewChangeListener(_particHolder)
                    else:
                        LOG.error("Unsupport notificationType:" + str(notificationType.toString()))




    def onBecomeStandbyFromLeader(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "STANDBY", from = "LEADER")



        """
        LOG.info("Become STANDBY from LEADER")
        # HelixManager
        manager = context.getManager()
        manager.removeListener(_particHolder)


    def onBecomeOfflineFromStandby(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext
        # Annotation @Transition(to = "OFFLINE", from = "STANDBY")



        """
        LOG.info("Become OFFLINE from STANDBY")



