# package org.apache.helix
#from org.apache.helix import *
#from org.apache.helix.PropertyType import ALERTS
#from org.apache.helix.PropertyType import ALERT_HISTORY
#from org.apache.helix.PropertyType import ALERT_STATUS
#from org.apache.helix.PropertyType import CONFIGS
#from org.apache.helix.PropertyType import CONTROLLER
#from org.apache.helix.PropertyType import CURRENTSTATES
#from org.apache.helix.PropertyType import ERRORS
#from org.apache.helix.PropertyType import ERRORS_CONTROLLER
#from org.apache.helix.PropertyType import EXTERNALVIEW
#from org.apache.helix.PropertyType import HEALTHREPORT
#from org.apache.helix.PropertyType import HISTORY
#from org.apache.helix.PropertyType import IDEALSTATES
#from org.apache.helix.PropertyType import LEADER
#from org.apache.helix.PropertyType import LIVEINSTANCES
#from org.apache.helix.PropertyType import MESSAGES
#from org.apache.helix.PropertyType import MESSAGES_CONTROLLER
#from org.apache.helix.PropertyType import PAUSE
#from org.apache.helix.PropertyType import PERSISTENTSTATS
#from org.apache.helix.PropertyType import STATEMODELDEFS
#from org.apache.helix.PropertyType import STATUSUPDATES
#from org.apache.helix.PropertyType import STATUSUPDATES_CONTROLLER
#from java.util import Arrays
#from java.util.concurrent import TimeUnit
#from org.apache.log4j import Logger
from org.apache.helix.ConfigScope import ConfigScopeProperty
#from org.apache.helix.manager.zk.ZKHelixDataAccessor import ZKHelixDataAccessor
#from org.apache.helix.manager.zk.ZkBaseDataAccessor import ZkBaseDataAccessor
#from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.model.AlertHistory import AlertHistory
from org.apache.helix.model.AlertStatus import AlertStatus
from org.apache.helix.model.Alerts import Alerts
from org.apache.helix.model.ClusterConstraints import ClusterConstraints
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.Error import Error
from org.apache.helix.model.ExternalView import ExternalView
#from org.apache.helix.model.HealthStat import HealthStat
from org.apache.helix.model.IdealState import IdealState
from org.apache.helix.model.InstanceConfig import InstanceConfig
from org.apache.helix.model.LeaderHistory import LeaderHistory
from org.apache.helix.model.LiveInstance import LiveInstance
from org.apache.helix.model.Message import Message
from org.apache.helix.model.PauseSignal import PauseSignal
from org.apache.helix.model.PersistentStats import PersistentStats
from org.apache.helix.model.StateModelDefinition import StateModelDefinition
from org.apache.helix.model.StatusUpdate import StatusUpdate

import logging
from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.PropertyPathConfig import PropertyPathConfig
from org.apache.helix.PropertyType import PropertyType

class PropertyKey:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
#    LOG = Logger.getLogger(PropertyKey.class)
    LOG = get_logger(__name__)
    LOG.setLevel(logging.INFO)


    """

    Parameters:
        PropertyType type
        Class<? extends HelixProperty> typeClazz
        String... params
    """
    def __init__(self, type, typeClazz, *params):
        self._type = type
#        if params == None or params.length == 0 or Arrays.asList(params).contains(None):
        if not params:
            raise IllegalArgumentException("params cannot be null")


        self._params = params
        self._typeClazz = typeClazz


#    def hashCode(self):
#        """
#        Returns int
#        @Override
#
#
#        """
#        return super(null, self).hashCode()


    def getPath(self):
        """
        Returns String


        """
        # String
        clusterName = self._params[0]
        # String[]
#        subKeys = Arrays.copyOfRange(self._params, 1, self._params.length)
        subKeys = self._params[1:]
        # String
        path = PropertyPathConfig.getPath(self._type, clusterName, *subKeys)
        if path == None: 
#            self.LOG.error("Invalid property key with type:" + str(self._type)+ "subKeys:" + str(Arrays.toString(self._params)))
            self.LOG.error("Invalid property key with type:" + str(self._type)+ "subKeys:" + str(str(self._params)))

        return path


#    Java modifiers:
#         static


    def getType(self):
        """
        Returns PropertyType


        """
        return self._type


    def getParams(self):
        """
        Returns String[]


        """
        return self._params


    def getTypeClass(self):
        """
        Returns Class<? extends HelixProperty>


        """
        return self._typeClazz


    def main(args):
        """
        Returns void
        Parameters:
            args: String[]
        Java modifiers:
             static

        """
        pass
#        # ZkClient
#        zkClient = ZkClient("localhost:2181")
#        zkClient.waitUntilConnected(10, TimeUnit.SECONDS)
#        # BaseDataAccessor
#        baseDataAccessor = ZkBaseDataAccessor(zkClient)
#        # HelixDataAccessor
#        accessor = ZKHelixDataAccessor("test-cluster", baseDataAccessor)
#        # Builder
#        builder = PropertyKey.Builder("test-cluster")
#        # HelixProperty
#        value = IdealState("test-resource")
#        accessor.createProperty(builder.idealStates("test-resource"), value)


    class Builder:



        """

        Parameters:
            String clusterName
        """
        def __init__(self, clusterName):
            self._clusterName = clusterName


        def idealStates(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.IDEALSTATES, IdealState, self._clusterName)


        def idealStates(self, resourceName):
            """
            Returns PropertyKey
            Parameters:
                resourceName: String


            """
            return PropertyKey(PropertyType.IDEALSTATES, IdealState, self._clusterName, resourceName)


        def stateModelDefs(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.STATEMODELDEFS, StateModelDefinition, self._clusterName)


        def stateModelDef(self, stateModelName):
            """
            Returns PropertyKey
            Parameters:
                stateModelName: String


            """
            return PropertyKey(PropertyType.STATEMODELDEFS, StateModelDefinition, self._clusterName, stateModelName)


        def clusterConfig(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.CONFIGS, None, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.CLUSTER))


        def instanceConfigs(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.CONFIGS, InstanceConfig, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.PARTICIPANT))


        def instanceConfig(self, instanceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: String


            """
            return PropertyKey(PropertyType.CONFIGS, InstanceConfig, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.PARTICIPANT), instanceName)


        def resourceConfig(self, resourceName):
            """
            Returns PropertyKey
            Parameters:
                resourceName: String


            """
            return PropertyKey(PropertyType.CONFIGS, None, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.RESOURCE), resourceName)


        def resourceConfig(self, instanceName, resourceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringresourceName: String


            """
            return PropertyKey(PropertyType.CONFIGS, None, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.RESOURCE), resourceName)


        def partitionConfig(self, resourceName, partitionName):
            """
            Returns PropertyKey
            Parameters:
                resourceName: StringpartitionName: String


            """
            return PropertyKey(PropertyType.CONFIGS, None, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.RESOURCE), resourceName)


        def partitionConfig(self, instanceName, resourceName, partitionName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringresourceName: StringpartitionName: String


            """
            return PropertyKey(PropertyType.CONFIGS, None, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.RESOURCE), resourceName)


        def constraints(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.CONFIGS, ClusterConstraints, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.CONSTRAINT))


        def constraint(self, constraintType):
            """
            Returns PropertyKey
            Parameters:
                constraintType: String


            """
            return PropertyKey(PropertyType.CONFIGS, ClusterConstraints, self._clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.CONSTRAINT), constraintType)


        def liveInstances(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.LIVEINSTANCES, LiveInstance, self._clusterName)


        def liveInstance(self, instanceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: String


            """
            return PropertyKey(PropertyType.LIVEINSTANCES, LiveInstance, self._clusterName, instanceName)


        def instances(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.CONFIGS, None, self._clusterName)


        def messages(self, instanceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: String


            """
            return PropertyKey(PropertyType.MESSAGES, Message, self._clusterName, instanceName)


        def message(self, instanceName, messageId):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringmessageId: String


            """
            return PropertyKey(PropertyType.MESSAGES, Message, self._clusterName, instanceName, messageId)


        def sessions(self, instanceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: String


            """
            return PropertyKey(PropertyType.CURRENTSTATES, CurrentState, self._clusterName, instanceName)


        def currentStates(self, instanceName, sessionId):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: String


            """
            return PropertyKey(PropertyType.CURRENTSTATES, CurrentState, self._clusterName, instanceName, sessionId)


#        def currentState(self, instanceName, sessionId, resourceName):
#            """
#            Returns PropertyKey
#            Parameters:
#                instanceName: StringsessionId: StringresourceName: String
#
#
#            """
#            return PropertyKey(PropertyType.CURRENTSTATES, CurrentState, self._clusterName, instanceName, sessionId, resourceName)


        def currentState(self, instanceName, sessionId, resourceName, bucketName=None):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: StringresourceName: StringbucketName: String


            """
            if bucketName == None:
                return PropertyKey(PropertyType.CURRENTSTATES, CurrentState, self._clusterName, instanceName, sessionId, resourceName)
            else:
                return PropertyKey(PropertyType.CURRENTSTATES, CurrentState, self._clusterName, instanceName, sessionId, resourceName, bucketName)


        def stateTransitionStatus(self, *args):
            if len(args)==4: return self.stateTransitionStatus_4(*args)
            elif len(args)==3: return self.stateTransitionStatus_3(*args)
            elif len(args)==2: return self.stateTransitionStatus_2(*args)
            else:
                raise IllegalArgumentException("Input arguments not supported. args = %s" % args)


        def stateTransitionStatus_4(self, instanceName, sessionId, resourceName, partitionName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: StringresourceName: StringpartitionName: String


            """
            return PropertyKey(PropertyType.STATUSUPDATES, StatusUpdate, self._clusterName, instanceName, sessionId, resourceName, partitionName)


        def stateTransitionStatus_3(self, instanceName, sessionId, resourceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: StringresourceName: String


            """
            return PropertyKey(PropertyType.STATUSUPDATES, StatusUpdate, self._clusterName, instanceName, sessionId, resourceName)


        def stateTransitionStatus_2(self, instanceName, sessionId):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: String


            """
            return PropertyKey(PropertyType.STATUSUPDATES, StatusUpdate, self._clusterName, instanceName, sessionId)


        def taskStatus(self, instanceName, sessionId, msgType, msgId):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: StringmsgType: StringmsgId: String


            """
            return PropertyKey(PropertyType.STATUSUPDATES, StatusUpdate, self._clusterName, instanceName, sessionId, msgType, msgId)


        def stateTransitionError(self, instanceName, sessionId, resourceName, partitionName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: StringresourceName: StringpartitionName: String


            """
            return PropertyKey(PropertyType.ERRORS, Error, self._clusterName, instanceName, sessionId, resourceName, partitionName)


        def stateTransitionErrors(self, instanceName, sessionId, resourceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: StringresourceName: String


            """
            return PropertyKey(PropertyType.ERRORS, Error, self._clusterName, instanceName, sessionId, resourceName)


        def taskError(self, instanceName, sessionId, msgType, msgId):
            """
            Returns PropertyKey
            Parameters:
                instanceName: StringsessionId: StringmsgType: StringmsgId: String


            """
            return PropertyKey(PropertyType.ERRORS, None, self._clusterName, instanceName, sessionId, msgType, msgId)


        def externalViews(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.EXTERNALVIEW, ExternalView, self._clusterName)


        def externalView(self, resourceName):
            """
            Returns PropertyKey
            Parameters:
                resourceName: String


            """
            return PropertyKey(PropertyType.EXTERNALVIEW, ExternalView, self._clusterName, resourceName)


        def controller(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.CONTROLLER, None, self._clusterName)


        def controllerTaskErrors(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.ERRORS_CONTROLLER, StatusUpdate, self._clusterName)


        def controllerTaskError(self, messageType, messageId):
            """
            Returns PropertyKey
            Parameters:
                messageType: StringmessageId: String


            """
            return PropertyKey(PropertyType.ERRORS_CONTROLLER, Error, self._clusterName, messageType, messageId)


        def controllerTaskStatuses(self, messageType):
            """
            Returns PropertyKey
            Parameters:
                messageType: String


            """
            return PropertyKey(PropertyType.STATUSUPDATES_CONTROLLER, StatusUpdate, self._clusterName, messageType)


        def controllerTaskStatus(self, messageType, messageId):
            """
            Returns PropertyKey
            Parameters:
                messageType: StringmessageId: String


            """
            return PropertyKey(PropertyType.STATUSUPDATES_CONTROLLER, StatusUpdate, self._clusterName, messageType, messageId)


        def controllerMessages(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.MESSAGES_CONTROLLER, Message, self._clusterName)


        def controllerMessage(self, msgId):
            """
            Returns PropertyKey
            Parameters:
                msgId: String


            """
            return PropertyKey(PropertyType.MESSAGES_CONTROLLER, Message, self._clusterName, msgId)


        def controllerLeaderHistory(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.HISTORY, LeaderHistory, self._clusterName)


        def controllerLeader(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.LEADER, LiveInstance, self._clusterName)


        def pause(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.PAUSE, PauseSignal, self._clusterName)


        def persistantStat(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.PERSISTENTSTATS, PersistentStats, self._clusterName)


        def alerts(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.ALERTS, Alerts, self._clusterName)


        def alertStatus(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.ALERT_STATUS, AlertStatus, self._clusterName)


        def alertHistory(self):
            """
            Returns PropertyKey


            """
            return PropertyKey(PropertyType.ALERT_HISTORY, AlertHistory, self._clusterName)


        def healthReport(self, instanceName, id):
            """
            Returns PropertyKey
            Parameters:
                instanceName: Stringid: String


            """
            raise BaseException("Not implemented")
    #        return PropertyKey(PropertyType.HEALTHREPORT, HealthStat, self._clusterName, instanceName, id)


        def healthReports(self, instanceName):
            """
            Returns PropertyKey
            Parameters:
                instanceName: String


            """
            raise BaseException("Not implemented")
    #        return PropertyKey(PropertyType.HEALTHREPORT, HealthStat, self._clusterName, instanceName)


