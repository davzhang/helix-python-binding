# package org.apache.helix
#from org.apache.helix import *
#from org.apache.log4j import Logger

#from org.apache.helix.manager.file.DynamicFileHelixManager import DynamicFileHelixManager
#from org.apache.helix.manager.file.StaticFileHelixManager import StaticFileHelixManager
from org.apache.helix.manager.zk.ZKHelixManager import ZKHelixManager
#from org.apache.helix.store.file.FilePropertyStore import FilePropertyStore
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.util.UserExceptions import IllegalArgumentException

#Java modifiers:
#     final
class HelixManagerFactory:

    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
#    logger = Logger.getLogger(HelixManagerFactory.class)

    @staticmethod
    def getZKHelixManager(clusterName, instanceName, type, zkAddr):
        """
        Returns HelixManager
        Parameters:
            clusterName: StringinstanceName: Stringtype: InstanceTypezkAddr: String
        Java modifiers:
             static

        Throws: 
            Exception
        """
        return ZKHelixManager(clusterName, instanceName, type, zkAddr)

    @staticmethod
    def getStaticFileHelixManager(clusterName, instanceName, type, clusterViewFile):
        """
        Returns HelixManager
        Parameters:
            clusterName: StringinstanceName: Stringtype: InstanceTypeclusterViewFile: String
        @Deprecated
        Java modifiers:
             static

        Throws: 
            Exception
        """
        if type != InstanceType.PARTICIPANT: 
            raise IllegalArgumentException("Static file-based cluster manager doesn't support type other than participant")


        return StaticFileHelixManager(clusterName, instanceName, type, clusterViewFile)

    @staticmethod
    def getDynamicFileHelixManager(clusterName, instanceName, type, store):
        """
        Returns HelixManager
        Parameters:
            clusterName: StringinstanceName: Stringtype: InstanceTypestore: FilePropertyStore<ZNRecord>
        @Deprecated
        Java modifiers:
             static

        Throws: 
            Exception
        """
        if type != InstanceType.PARTICIPANT and type != InstanceType.CONTROLLER:
            raise IllegalArgumentException("Dynamic file-based cluster manager doesn't support types other than participant and controller")


        return DynamicFileHelixManager(clusterName, instanceName, type, store)



