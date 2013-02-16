# package org.apache.helix
#from org.apache.helix import *

from org.apache.helix.util.misc import enum, explicit_enum

Type = enum('CLUSTER', 'INSTANCE', 'CONTROLLER', 'RESOURCE')


class PropertyTypeProps:


    
    """
    Java modifiers:
         private
    Parameters:
        Type type
        boolean isPersistent
        boolean mergeOnUpdate
        boolean updateOnlyOnExists
        boolean createOnlyIfAbsent
        boolean isCached
        boolean isAsyncWrite
    """
    
    def __init__(self, type, isPersistent=False, mergeOnUpdate=False, updateOnlyOnExists=False, createOnlyIfAbsent=False, isCached=False, isAsyncWrite=False):
        self.type = type
        self.isPersistentInternal = isPersistent
        self.mergeOnUpdate = mergeOnUpdate
        self.updateOnlyOnExists = updateOnlyOnExists
        self.createOnlyIfAbsent = createOnlyIfAbsent
        self.isCached = isCached
        self.usingPropertyTransferServer = isAsyncWrite


    def isCreateOnlyIfAbsent(self):
        """
        Returns boolean


        """
        return self.createOnlyIfAbsent


    def setCreateIfAbsent(self, createIfAbsent):
        """
        Returns void
        Parameters:
            createIfAbsent: boolean


        """
        self.createOnlyIfAbsent = createIfAbsent


    def getType(self):
        """
        Returns Type


        """
        return self.type


    def setType(self, type):
        """
        Returns void
        Parameters:
            type: Type


        """
        self.type = type


    def isPersistent(self):
        """
        Returns boolean


        """
        return self.isPersistentInternal


    def setPersistent(self, isPersistent):
        """
        Returns void
        Parameters:
            isPersistent: boolean


        """
        self.isPersistentInternal = isPersistent


    def isMergeOnUpdate(self):
        """
        Returns boolean


        """
        return self.mergeOnUpdate


    def setMergeOnUpdate(self, mergeOnUpdate):
        """
        Returns void
        Parameters:
            mergeOnUpdate: boolean


        """
        self.mergeOnUpdate = mergeOnUpdate


    def isUpdateOnlyOnExists(self):
        """
        Returns boolean


        """
        return self.updateOnlyOnExists


    def setUpdateOnlyOnExists(self, updateOnlyOnExists):
        """
        Returns void
        Parameters:
            updateOnlyOnExists: boolean


        """
        self.updateOnlyOnExists = updateOnlyOnExists


    def isCached(self):
        """
        Returns boolean


        """
        return self.isCached


    def usePropertyTransferServer(self):
        """
        Returns boolean


        """
        return self.usingPropertyTransferServer

#CONFIGS=PropertyType(Type.CLUSTER, True, False, False, False, True)
#LIVEINSTANCES=PropertyType(Type.CLUSTER, False, False, False, True, True)
#INSTANCES=PropertyType(Type.CLUSTER, True, False)
#IDEALSTATES=PropertyType(Type.CLUSTER, True, False, False, False, True)
#EXTERNALVIEW=PropertyType(Type.CLUSTER, True, False)
#STATEMODELDEFS=PropertyType(Type.CLUSTER, True, False, False, False, True)
#CONTROLLER=PropertyType(Type.CLUSTER, True, False)
#PROPERTYSTORE=PropertyType(Type.CLUSTER, True, False)
#HELIX_PROPERTYSTORE=PropertyType(Type.CLUSTER, True, False)
#MESSAGES=PropertyType(Type.INSTANCE, True, True, True)
#CURRENTSTATES=PropertyType(Type.INSTANCE, True, True, False, False, True)
#STATUSUPDATES=PropertyType(Type.INSTANCE, True, True, False, False, False, True)
#ERRORS=PropertyType(Type.INSTANCE, True, True)
#HEALTHREPORT=PropertyType(Type.INSTANCE, True, False, False, False, False, True)
#LEADER=PropertyType(Type.CONTROLLER, False, False, True, True)
#HISTORY=PropertyType(Type.CONTROLLER, True, True, True)
#PAUSE=PropertyType(Type.CONTROLLER, True, False, True)
#MESSAGES_CONTROLLER=PropertyType(Type.CONTROLLER, True, False, True)
#STATUSUPDATES_CONTROLLER=PropertyType(Type.CONTROLLER, True, True, True)
#ERRORS_CONTROLLER=PropertyType(Type.CONTROLLER, True, True, True)
#PERSISTENTSTATS=PropertyType(Type.CONTROLLER, True, False, False, False)
#ALERTS=PropertyType(Type.CONTROLLER, True, False, False, False)
#ALERT_STATUS=PropertyType(Type.CONTROLLER, True, False, False, False)
#ALERT_HISTORY=PropertyType(Type.CONTROLLER, True, False, False, False)

PropertyType = explicit_enum(
    CONFIGS=PropertyTypeProps(Type.CLUSTER, True, False, False, False, True),
    LIVEINSTANCES=PropertyTypeProps(Type.CLUSTER, False, False, False, True, True),
    INSTANCES=PropertyTypeProps(Type.CLUSTER, True, False),
    IDEALSTATES=PropertyTypeProps(Type.CLUSTER, True, False, False, False, True),
    EXTERNALVIEW=PropertyTypeProps(Type.CLUSTER, True, False),
    STATEMODELDEFS=PropertyTypeProps(Type.CLUSTER, True, False, False, False, True),
    CONTROLLER=PropertyTypeProps(Type.CLUSTER, True, False),
    PROPERTYSTORE=PropertyTypeProps(Type.CLUSTER, True, False),
    HELIX_PROPERTYSTORE=PropertyTypeProps(Type.CLUSTER, True, False),
    MESSAGES=PropertyTypeProps(Type.INSTANCE, True, True, True),
    CURRENTSTATES=PropertyTypeProps(Type.INSTANCE, True, True, False, False, True),
    STATUSUPDATES=PropertyTypeProps(Type.INSTANCE, True, True, False, False, False, True),
    ERRORS=PropertyTypeProps(Type.INSTANCE, True, True),
    HEALTHREPORT=PropertyTypeProps(Type.INSTANCE, True, False, False, False, False, True),
    LEADER=PropertyTypeProps(Type.CONTROLLER, False, False, True, True),
    HISTORY=PropertyTypeProps(Type.CONTROLLER, True, True, True),
    PAUSE=PropertyTypeProps(Type.CONTROLLER, True, False, True),
    MESSAGES_CONTROLLER=PropertyTypeProps(Type.CONTROLLER, True, False, True),
    STATUSUPDATES_CONTROLLER=PropertyTypeProps(Type.CONTROLLER, True, True, True),
    ERRORS_CONTROLLER=PropertyTypeProps(Type.CONTROLLER, True, True, True),
    PERSISTENTSTATS=PropertyTypeProps(Type.CONTROLLER, True, False, False, False),
    ALERTS=PropertyTypeProps(Type.CONTROLLER, True, False, False, False),
    ALERT_STATUS=PropertyTypeProps(Type.CONTROLLER, True, False, False, False),
    ALERT_HISTORY=PropertyTypeProps(Type.CONTROLLER, True, False, False, False)
)
