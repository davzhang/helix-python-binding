# package org.apache.helix.participant
#from org.apache.helix.participant import *
#from java.util import List
#from org.apache.log4j import Logger
from org.apache.helix.ConfigChangeListener import ConfigChangeListener
from org.apache.helix.ExternalViewChangeListener import ExternalViewChangeListener
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.LiveInstanceChangeListener import LiveInstanceChangeListener
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.NotificationContext import Type
from org.apache.helix.PropertyKey import Builder
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.ExternalView import ExternalView
from org.apache.helix.model.InstanceConfig import InstanceConfig
from org.apache.helix.model.LiveInstance import LiveInstance


class CustomCodeInvoker(LiveInstanceChangeListener, ConfigChangeListener, ExternalViewChangeListener):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(CustomCodeInvoker.class)





    """

    Parameters:
        CustomCodeCallbackHandler callback
        String partitionKey
    """
    def __init__(self, callback, partitionKey):
        self._callback = callback
        self._partitionKey = partitionKey


    def callParticipantCode(self, context):
        """
        Returns void
        Parameters:
            context: NotificationContext
        Java modifiers:
             private

        """
        if context.getType() == Type.INIT or context.getType() == Type.CALLBACK: 
            if context.getType() == Type.CALLBACK: 
                # HelixManager
                manager = context.getManager()
                # HelixDataAccessor
                accessor = manager.getHelixDataAccessor()
                # Builder
                keyBuilder = accessor.keyBuilder()
                # String
                instance = manager.getInstanceName()
                # String
                sessionId = manager.getSessionId()
                # String
                resourceName = _partitionKey.substring(0, _partitionKey.lastIndexOf('_'))
                # CurrentState
                curState = accessor.getProperty(keyBuilder.currentState(instance, sessionId, resourceName))
                if curState == None: 
                    return

                # String
                state = curState.getState(_partitionKey)
                if state == None or not state.equalsIgnoreCase("LEADER"): 
                    return


            try:
                _callback.onCallback(context)
            except Exception, e:
                LOG.error("Error invoking callback:" + str(_callback)+ str(e))




    def onLiveInstanceChange(self, liveInstances, changeContext):
        """
        Returns void
        Parameters:
            liveInstances: List<LiveInstance>changeContext: NotificationContext
        @Override


        """
        LOG.info("onLiveInstanceChange() invoked")
        callParticipantCode(changeContext)


    def onConfigChange(self, configs, changeContext):
        """
        Returns void
        Parameters:
            configs: List<InstanceConfig>changeContext: NotificationContext
        @Override


        """
        LOG.info("onConfigChange() invoked")
        callParticipantCode(changeContext)


    def onExternalViewChange(self, externalViewList, changeContext):
        """
        Returns void
        Parameters:
            externalViewList: List<ExternalView>changeContext: NotificationContext
        @Override


        """
        LOG.info("onExternalViewChange() invoked")
        callParticipantCode(changeContext)



