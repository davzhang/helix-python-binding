# package org.apache.helix
#from org.apache.helix import *
import re
from org.apache.helix.PropertyType import PropertyType, Type
#from java.util import Arrays
#from java.util import HashMap
#from java.util import Map
#from java.util.regex import Matcher
#from java.util.regex import Pattern
#from org.apache.log4j import Logger
from org.apache.helix.model.AlertStatus import AlertStatus
from org.apache.helix.model.Alerts import Alerts
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.ExternalView import ExternalView
#from org.apache.helix.model.HealthStat import HealthStat
from org.apache.helix.model.IdealState import IdealState
from org.apache.helix.model.InstanceConfig import InstanceConfig
from org.apache.helix.model.LeaderHistory import LeaderHistory
from org.apache.helix.model.LiveInstance import LiveInstance
from org.apache.helix.model.Message import Message
from org.apache.helix.model.PauseSignal import PauseSignal
from org.apache.helix.model.StateModelDefinition import StateModelDefinition
from org.apache.helix.model.StatusUpdate import StatusUpdate

from org.apache.helix.util.logger import get_logger

class PropertyPathConfig:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """
    Java modifiers:
         static
    Type:
        Map<PropertyType, Map<Integer, String>>
    """
    templateMap = {}
#    templateMap = HashMap<PropertyType, Map<Integer, String>>()

    """
    Java modifiers:
         static
    Type:
        Map<PropertyType, Class<? extends HelixProperty>>
    """
    typeToClassMapping = {}
#    typeToClassMapping = HashMap<PropertyType, Class<? extends HelixProperty>>()

    typeToClassMapping.__setitem__(PropertyType.LIVEINSTANCES, LiveInstance)
    typeToClassMapping.__setitem__(PropertyType.IDEALSTATES, IdealState)
    typeToClassMapping.__setitem__(PropertyType.CONFIGS, InstanceConfig)
    typeToClassMapping.__setitem__(PropertyType.EXTERNALVIEW, ExternalView)
    typeToClassMapping.__setitem__(PropertyType.STATEMODELDEFS, StateModelDefinition)
    typeToClassMapping.__setitem__(PropertyType.MESSAGES, Message)
    typeToClassMapping.__setitem__(PropertyType.CURRENTSTATES, CurrentState)
    typeToClassMapping.__setitem__(PropertyType.STATUSUPDATES, StatusUpdate)
    typeToClassMapping.__setitem__(PropertyType.HISTORY, LeaderHistory)
#    typeToClassMapping.__setitem__(PropertyType.HEALTHREPORT, HealthStat)
    typeToClassMapping.__setitem__(PropertyType.ALERTS, Alerts)
    typeToClassMapping.__setitem__(PropertyType.ALERT_STATUS, AlertStatus)
    typeToClassMapping.__setitem__(PropertyType.PAUSE, PauseSignal)




    """
    Java modifiers:
         static
    Type:
        Pattern
    """
    pattern = re.compile("({.+?})")

    @staticmethod
    def addEntry(type, numKeys, template):
        """
        Returns void
        Parameters:
            type: PropertyTypenumKeys: inttemplate: String
        Java modifiers:
             private static

        """
        if not PropertyPathConfig.templateMap.__contains__(type):
            PropertyPathConfig.templateMap.__setitem__(type, {})
#            PropertyPathConfig.templateMap.__setitem__(type, HashMap<Integer, String>())

        PropertyPathConfig.logger.trace("Adding template for type:" + str(type.getType()) + " arguments:" + str(numKeys) + " template:" + template)
        PropertyPathConfig.templateMap.get(type).__setitem__(numKeys, template)

    @staticmethod
    def getPath(type, clusterName, *keys):
        """
        Returns String
        Parameters:
            type: PropertyTypeclusterName: Stringkeys: String
        Java modifiers:
             static

        """
        if clusterName == None: 
            PropertyPathConfig.logger.warn("ClusterName can't be null for type:" + type)
            return None

        if keys == None: 
            keys = []
#            keys = new String[] {}

        # String
        template = None
        if PropertyPathConfig.templateMap.__contains__(type):
            template = PropertyPathConfig.templateMap.__getitem__(type).get(len(keys) + 1)

        # String
        result = None
        if template != None: 
            result = template
            # Matcher
            matches = PropertyPathConfig.pattern.findall(template)
#            matcher = pattern.matcher(template)
            # int
            count = 0
#            while (matcher.find():
            for var in matches:
                count = count + 1
                # String
#                var = matcher.group()
                if count == 1:
                    result = result.replace(var, clusterName)
                else:
                    result = result.replace(var, keys[count - 2])



        if result == None or result.find('{') > -1 or result.find('}') > -1:
            try:
                PropertyPathConfig.logger.warn("Unable to instantiate template:" + template + " using clusterName:" + clusterName + " and keys:" + str(keys))
            except:
                raise

        return result

    @staticmethod
    def getInstanceNameFromPath(path):
        """
        Returns String
        Parameters:
            path: String
        Java modifiers:
             static

        """
        instancePath = PropertyType.toString(PropertyType.INSTANCES)
        if path.find("/" + instancePath + "/") != -1:
            # String[]
            split = path.split("/")
            if len(split) > 3:
                return split[3]


        return None

PropertyPathConfig.addEntry(PropertyType.CONFIGS, 1, "/{clusterName}/CONFIGS")
PropertyPathConfig.addEntry(PropertyType.CONFIGS, 2, "/{clusterName}/CONFIGS/{scope}")
PropertyPathConfig.addEntry(PropertyType.CONFIGS, 3, "/{clusterName}/CONFIGS/{scope}/{scopeKey}")
PropertyPathConfig.addEntry(PropertyType.LIVEINSTANCES, 1, "/{clusterName}/LIVEINSTANCES")
PropertyPathConfig.addEntry(PropertyType.LIVEINSTANCES, 2, "/{clusterName}/LIVEINSTANCES/{instanceName}")
PropertyPathConfig.addEntry(PropertyType.INSTANCES, 1, "/{clusterName}/INSTANCES")
PropertyPathConfig.addEntry(PropertyType.INSTANCES, 2, "/{clusterName}/INSTANCES/{instanceName}")
PropertyPathConfig.addEntry(PropertyType.IDEALSTATES, 1, "/{clusterName}/IDEALSTATES")
PropertyPathConfig.addEntry(PropertyType.IDEALSTATES, 2, "/{clusterName}/IDEALSTATES/{resourceName}")
PropertyPathConfig.addEntry(PropertyType.EXTERNALVIEW, 1, "/{clusterName}/EXTERNALVIEW")
PropertyPathConfig.addEntry(PropertyType.EXTERNALVIEW, 2, "/{clusterName}/EXTERNALVIEW/{resourceName}")
PropertyPathConfig.addEntry(PropertyType.STATEMODELDEFS, 1, "/{clusterName}/STATEMODELDEFS")
PropertyPathConfig.addEntry(PropertyType.STATEMODELDEFS, 2, "/{clusterName}/STATEMODELDEFS/{stateModelName}")
PropertyPathConfig.addEntry(PropertyType.CONTROLLER, 1, "/{clusterName}/CONTROLLER")
PropertyPathConfig.addEntry(PropertyType.PROPERTYSTORE, 1, "/{clusterName}/PROPERTYSTORE")
PropertyPathConfig.addEntry(PropertyType.HELIX_PROPERTYSTORE, 1, "/{clusterName}/HELIX_PROPERTYSTORE")
PropertyPathConfig.addEntry(PropertyType.MESSAGES, 2, "/{clusterName}/INSTANCES/{instanceName}/MESSAGES")
PropertyPathConfig.addEntry(PropertyType.MESSAGES, 3, "/{clusterName}/INSTANCES/{instanceName}/MESSAGES/{msgId}")
PropertyPathConfig.addEntry(PropertyType.CURRENTSTATES, 2, "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES")
PropertyPathConfig.addEntry(PropertyType.CURRENTSTATES, 3, "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}")
PropertyPathConfig.addEntry(PropertyType.CURRENTSTATES, 4, "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceName}")
PropertyPathConfig.addEntry(PropertyType.CURRENTSTATES, 5, "/{clusterName}/INSTANCES/{instanceName}/CURRENTSTATES/{sessionId}/{resourceName}/{bucketName}")
PropertyPathConfig.addEntry(PropertyType.STATUSUPDATES, 2, "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES")
PropertyPathConfig.addEntry(PropertyType.STATUSUPDATES, 3, "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}")
PropertyPathConfig.addEntry(PropertyType.STATUSUPDATES, 4, "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}")
PropertyPathConfig.addEntry(PropertyType.STATUSUPDATES, 5, "/{clusterName}/INSTANCES/{instanceName}/STATUSUPDATES/{sessionId}/{subPath}/{recordName}")
PropertyPathConfig.addEntry(PropertyType.ERRORS, 2, "/{clusterName}/INSTANCES/{instanceName}/ERRORS")
PropertyPathConfig.addEntry(PropertyType.ERRORS, 3, "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}")
PropertyPathConfig.addEntry(PropertyType.ERRORS, 4, "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}")
PropertyPathConfig.addEntry(PropertyType.ERRORS, 5, "/{clusterName}/INSTANCES/{instanceName}/ERRORS/{sessionId}/{subPath}/{recordName}")
PropertyPathConfig.addEntry(PropertyType.HEALTHREPORT, 2, "/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT")
PropertyPathConfig.addEntry(PropertyType.HEALTHREPORT, 3, "/{clusterName}/INSTANCES/{instanceName}/HEALTHREPORT/{reportName}")
PropertyPathConfig.addEntry(PropertyType.MESSAGES_CONTROLLER, 1, "/{clusterName}/CONTROLLER/MESSAGES")
PropertyPathConfig.addEntry(PropertyType.MESSAGES_CONTROLLER, 2, "/{clusterName}/CONTROLLER/MESSAGES/{msgId}")
PropertyPathConfig.addEntry(PropertyType.ERRORS_CONTROLLER, 1, "/{clusterName}/CONTROLLER/ERRORS")
PropertyPathConfig.addEntry(PropertyType.ERRORS_CONTROLLER, 2, "/{clusterName}/CONTROLLER/ERRORS/{errorId}")
PropertyPathConfig.addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 1, "/{clusterName}/CONTROLLER/STATUSUPDATES")
PropertyPathConfig.addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 2, "/{clusterName}/CONTROLLER/STATUSUPDATES/{subPath}")
PropertyPathConfig.addEntry(PropertyType.STATUSUPDATES_CONTROLLER, 3, "/{clusterName}/CONTROLLER/STATUSUPDATES/{subPath}/{recordName}")
PropertyPathConfig.addEntry(PropertyType.LEADER, 1, "/{clusterName}/CONTROLLER/LEADER")
PropertyPathConfig.addEntry(PropertyType.HISTORY, 1, "/{clusterName}/CONTROLLER/HISTORY")
PropertyPathConfig.addEntry(PropertyType.PAUSE, 1, "/{clusterName}/CONTROLLER/PAUSE")
PropertyPathConfig.addEntry(PropertyType.PERSISTENTSTATS, 1, "/{clusterName}/CONTROLLER/PERSISTENTSTATS")
PropertyPathConfig.addEntry(PropertyType.ALERTS, 1, "/{clusterName}/CONTROLLER/ALERTS")
PropertyPathConfig.addEntry(PropertyType.ALERT_STATUS, 1, "/{clusterName}/CONTROLLER/ALERT_STATUS")
PropertyPathConfig.addEntry(PropertyType.ALERT_HISTORY, 1, "/{clusterName}/CONTROLLER/ALERT_HISTORY")


