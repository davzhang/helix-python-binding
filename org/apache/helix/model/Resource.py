# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import Collection
#from java.util import LinkedHashMap
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.HelixConstants import HelixConstants


class Resource:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(Resource.class)













    """

    Parameters:
        String resourceName
    """
    def __init__(self, resourceName):
        self._resourceName = resourceName
        self._partitionMap = LinkedHashMap<String, Partition>()


    def getStateModelDefRef(self):
        """
        Returns String


        """
        return _stateModelDefRef


    def setStateModelDefRef(self, stateModelDefRef):
        """
        Returns void
        Parameters:
            stateModelDefRef: String


        """
        _stateModelDefRef = stateModelDefRef


    def setStateModelFactoryName(self, factoryName):
        """
        Returns void
        Parameters:
            factoryName: String


        """
        if factoryName == None: 
            _stateModelFactoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY
        else:
            _stateModelFactoryName = factoryName



    def getStateModelFactoryname(self):
        """
        Returns String


        """
        return _stateModelFactoryName


    def getResourceName(self):
        """
        Returns String


        """
        return _resourceName


    def getPartitions(self):
        """
        Returns Collection<Partition>


        """
        return _partitionMap.values()


    def addPartition(self, partitionName):
        """
        Returns void
        Parameters:
            partitionName: String


        """
        _partitionMap.put(partitionName, Partition(partitionName))


    def getPartition(self, partitionName):
        """
        Returns Partition
        Parameters:
            partitionName: String


        """
        return _partitionMap.get(partitionName)


    def getBucketSize(self):
        """
        Returns int


        """
        return _bucketSize


    def setBucketSize(self, bucketSize):
        """
        Returns void
        Parameters:
            bucketSize: int


        """
        _bucketSize = bucketSize


    def setGroupMessageMode(self, mode):
        """
        Returns void
        Parameters:
            mode: boolean


        """
        _groupMessageMode = mode


    def getGroupMessageMode(self):
        """
        Returns boolean


        """
        return _groupMessageMode


    def toString(self):
        """
        Returns String
        @Override


        """
        # StringBuilder
        sb = StringBuilder()
        sb.append("resourceName:").append(_resourceName)
        sb.append(", stateModelDef:").append(_stateModelDefRef)
        sb.append(", bucketSize:").append(_bucketSize)
        sb.append(", partitionStateMap:").append(_partitionMap)
        return sb.toString()



