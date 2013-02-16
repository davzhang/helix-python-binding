# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import ArrayList
#from java.util import Collections
#from java.util import List
#from java.util import Map
#from java.util import Set
#from java.util import TreeMap
#from java.util import TreeSet
#from org.apache.log4j import Logger
from org.apache.helix.HelixConstants import HelixConstants
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.ZNRecord import ZNRecord

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum, ternary

IdealStateProperty=enum('NUM_PARTITIONS', 'STATE_MODEL_DEF_REF', 'STATE_MODEL_FACTORY_NAME', 'REPLICAS', 'IDEAL_STATE_MODE', 'REBALANCE_TIMER_PERIOD')
IdealStateModeProperty = enum('AUTO', 'CUSTOMIZED', 'AUTO_REBALANCE')

class IdealState(HelixProperty):



    """
    Java modifiers:
         final static
    Type:
        String
    """
    QUERY_LIST = "PREFERENCE_LIST_QUERYS"



    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    def __init__(self, *args):
        super(IdealState,self).__init__(*args)

#    """
#
#    Parameters:
#        String resourceName
#    """
#    def __init__(self, resourceName):
#        super(resourceName)
#
#
#    """
#
#    Parameters:
#        ZNRecord record
#    """
#    def __init__(self, record):
#        super(record)


    def getResourceName(self):
        """
        Returns String


        """
        return self._record.getId()


    def setIdealStateMode(self, mode):
        """
        Returns void
        Parameters:
            mode: String


        """
        self._record.setSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString(), mode)


    def getIdealStateMode(self):
        """
        Returns IdealStateModeProperty


        """
        # String
        mode = self._record.getSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString())
        try:
            return IdealStateModeProperty.valueOf(mode)
        except Exception, e:
            return IdealStateModeProperty.AUTO



    def setPartitionState(self, partitionName, instanceName, state):
        """
        Returns void
        Parameters:
            partitionName: StringinstanceName: Stringstate: String


        """
        # Map<String, String>
        mapField = self._record.getMapField(partitionName)
        if mapField == None: 
            self._record.setMapField(partitionName, {})
#            self._record.setMapField(partitionName, TreeMap<String, String>())

        self._record.getMapField(partitionName).put(instanceName, state)


    def getPartitionSet(self):
        """
        Returns Set<String>


        """
        if self.getIdealStateMode() == IdealStateModeProperty.AUTO or self.getIdealStateMode() == IdealStateModeProperty.AUTO_REBALANCE:
            return self._record.getListFields().keySet()
        else:
            if self.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED:
                return self._record.getMapFields().keySet()
            else:
                self.logger.error("Invalid ideal state mode:" + str(self.getResourceName()))
                return set()
#                return Collections.emptySet()



    def getInstanceStateMap(self, partitionName):
        """
        Returns Map<String, String>
        Parameters:
            partitionName: String


        """
        return self._record.getMapField(partitionName)


    def getInstanceSet(self, partitionName):
        """
        Returns Set<String>
        Parameters:
            partitionName: String


        """
        if self.getIdealStateMode() == IdealStateModeProperty.AUTO or self.getIdealStateMode() == IdealStateModeProperty.AUTO_REBALANCE:
            # List<String>
            prefList = self._record.getListField(partitionName)
            if prefList != None: 
                return set(prefList)
#                return TreeSet<String>(prefList)
            else:
                self.logger.warn(partitionName + " does NOT exist")
                return set()

        else:
            if self.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED:
                # Map<String, String>
                stateMap = self._record.getMapField(partitionName)
                if stateMap != None: 
                    return set(stateMap.keys())
                else:
                    self.logger.warn(partitionName + " does NOT exist")
                    return set()

            else:
                self.logger.error("Invalid ideal state mode: " + str(self.getResourceName()))
                return set()



    def getPreferenceList(self, partitionName):
        """
        Returns List<String>
        Parameters:
            partitionName: String


        """
        # List<String>
        instanceStateList = self._record.getListField(partitionName)
        if instanceStateList != None: 
            return instanceStateList

        self.logger.warn("Resource key:" + partitionName + " does not have a pre-computed preference list.")
        return None


    def getStateModelDefRef(self):
        """
        Returns String


        """
        return self._record.getSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString())


    def setStateModelDefRef(self, stateModel):
        """
        Returns void
        Parameters:
            stateModel: String


        """
        self._record.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), stateModel)


    def setNumPartitions(self, numPartitions):
        """
        Returns void
        Parameters:
            numPartitions: int


        """
        self._record.setSimpleField(IdealStateProperty.toString(IdealStateProperty.NUM_PARTITIONS), str(numPartitions))


    def getNumPartitions(self):
        """
        Returns int


        """
        # String
        numPartitionStr = self._record.getSimpleField(IdealStateProperty.toString(IdealStateProperty.NUM_PARTITIONS))
        try:
            return int(numPartitionStr)
        except ValueError, e:
            self.logger.error("Can't parse number of partitions: " + str(numPartitionStr)+ str(e))
            return -1



    def setReplicas(self, replicas):
        """
        Returns void
        Parameters:
            replicas: String


        """
        self._record.setSimpleField(IdealStateProperty.REPLICAS.toString(), replicas)


    def getReplicas(self):
        """
        Returns String


        """
        # String
        replica = self._record.getSimpleField(IdealStateProperty.REPLICAS.toString())
        if replica == None: 
            # String
            firstPartition = None
            idealStateMode = self.getIdealStateMode()
            if idealStateMode == IdealStateModeProperty.AUTO:
                    if self._record.getListFields().size() == 0:
                        replica = "0"
                    else:
                        firstPartition = self._record.getListFields().keys()[0]
                        if firstPartition:
                            replica = str(self._record.getListField(firstPartition).size())
                        else: replica = 0

                    self.logger.warn("could NOT found replicas in idealState. Use size of the first list instead. replica: " + replica + ", 1st partition: " + firstPartition)
            elif idealStateMode == IdealStateModeProperty.CUSTOMIZED:
                    if self._record.getMapFields().size() == 0:
                        replica = "0"
                    else:
                        firstPartition = self._record.getMapFields().keys()[0]
                        if firstPartition:
                            replica = str(self._record.getListField(firstPartition).size())
                        else: replica = 0
#                        replica = str(ternary(firstPartition == None, 0, self._record.getMapField(firstPartition).size()))

                    self.logger.warn("could NOT found replicas in idealState. Use size of the first map instead. replica: " + replica + ", 1st partition: " + firstPartition)
            else:
                    replica = "0"
                    self.logger.error("could NOT determine replicas. set to 0")

        return replica


    def setStateModelFactoryName(self, name):
        """
        Returns void
        Parameters:
            name: String


        """
        self._record.setSimpleField(IdealStateProperty.STATE_MODEL_FACTORY_NAME.toString(), name)


    def getStateModelFactoryName(self):
        """
        Returns String


        """
        # String
        ftyName = self._record.getSimpleField(IdealStateProperty.STATE_MODEL_FACTORY_NAME.toString())
        if ftyName == None: 
            ftyName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY

        return ftyName


    def getRebalanceTimerPeriod(self):
        """
        Returns int


        """
        if self._record.getSimpleFields().containsKey(IdealStateProperty.REBALANCE_TIMER_PERIOD.toString()):
            try:
                # int
                result = int(self._record.getSimpleField(IdealStateProperty.REBALANCE_TIMER_PERIOD.toString()))
                return result
            except ValueError, e:
                self.logger.error(""+ str(e))


        return -1


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        if self.getNumPartitions() < 0:
            self.logger.error("idealState:" + str(self._record)+ " does not have number of partitions (was " + str(self.getNumPartitions())+ ").")
            return False

        if self.getStateModelDefRef() == None:
            self.logger.error("idealStates:" + str(self._record)+ " does not have state model definition.")
            return False

        if self.getIdealStateMode() == IdealStateModeProperty.AUTO and self.getReplicas() == None:
            self.logger.error("idealStates:" + str(self._record)+ " does not have replica.")
            return False

        return True



