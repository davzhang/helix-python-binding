# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import ArrayList
#from java.util import Collections
#from java.util import HashSet
#from java.util import List
#from java.util import Map
#from java.util import Set
#from org.apache.log4j import Logger
import socket
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.ZNRecord import ZNRecord

from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum

InstanceConfigProperty=enum('HELIX_HOST', 'HELIX_PORT', 'HELIX_ENABLED', 'HELIX_DISABLED_PARTITION')

class InstanceConfig(HelixProperty):



    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    _logger = get_logger(__name__)

    """

    Parameters:
        String id
    """
    def __init__(self, id):
        super(id)


    """

    Parameters:
        ZNRecord record
    """
    def __init__(self, record):
        super(record)


    def getHostName(self):
        """
        Returns String


        """
        return self._record.getSimpleField(InstanceConfigProperty.HELIX_HOST.toString())


    def setHostName(self, hostName):
        """
        Returns void
        Parameters:
            hostName: String


        """
        self._record.setSimpleField(InstanceConfigProperty.HELIX_HOST.toString(), hostName)


    def getPort(self):
        """
        Returns String


        """
        return self._record.getSimpleField(InstanceConfigProperty.HELIX_PORT.toString())


    def setPort(self, port):
        """
        Returns void
        Parameters:
            port: String


        """
        self._record.setSimpleField(InstanceConfigProperty.HELIX_PORT.toString(), port)


    def getInstanceEnabled(self):
        """
        Returns boolean


        """
        # String
        isEnabled = self._record.getSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString())
        return eval(isEnabled.lower().capitalize())
#        return Boolean.parseBoolean(isEnabled)


    def setInstanceEnabled(self, enabled):
        """
        Returns void
        Parameters:
            enabled: boolean


        """
        self._record.setSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString(), str(enabled))


    def getInstanceEnabledForPartition(self, partition):
        """
        Returns boolean
        Parameters:
            partition: String


        """
        # List<String>
        disabledPartitions = self._record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString())
        if disabledPartitions != None and disabledPartitions.contains(partition): 
            return False
        else:
            return True



    def getDisabledPartitionMap(self):
        """
        Returns Map<String, String>


        """
        return self._record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString())


    def setInstanceEnabledForPartition(self, partitionName, enabled):
        """
        Returns void
        Parameters:
            partitionName: Stringenabled: boolean


        """
        # List<String>
        list = self._record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString())
        # Set<String>
#        disabledPartitions = set()
#        disabledPartitions = HashSet<String>()
        if list != None:
            disabledPartitions = set(list)

        if enabled: 
            disabledPartitions.remove(partitionName)
        else:
            disabledPartitions.add(partitionName)

#        list = ArrayList<String>(disabledPartitions)
        list = sorted(list(disabledPartitions))
#        Collections.sort(list)
        self._record.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(), list)


    def __equals__(self, obj):
        """
        Returns boolean
        Parameters:
            obj: Object
        @Override


        """
        if type(obj) == InstanceConfig: 
            # InstanceConfig
#            that = (InstanceConfig) obj
            that = obj
            if (self.getHostName() == that.getHostName()) and (self.getPort() == that.getPort()):
                return True


        return False


    def hashCode(self):
        """
        Returns int
        @Override


        """
        # StringBuffer
#        sb = StringBuffer()
#        sb.append(self.getHostName())
#        sb.append("_")
#        sb.append(self.getPort())
        sb = "%s_%s" % (self.getHostName(), self.getPort())
#        return sb.toString().hashCode()
        return hash(sb)

    def getInstanceName(self):
        """
        Returns String


        """
        return self._record.getId()


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        if self.getHostName() == None:
            self._logger.error("instanceconfig does not have host name. id:" + str(self._record.getId()))
            return False

        if self.getPort() == None:
            self._logger.error("instanceconfig does not have host port. id:" + str(self._record.getId()))
            return False

        return True



