# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import DataInputStream
#from java.io import File
#from java.io import FileInputStream
#from java.io import IOException
#from java.net import InetAddress
#from java.net import UnknownHostException
#from java.util import ArrayList
#from java.util import Collections
#from java.util import HashMap
#from java.util import HashSet
#from java.util import List
#from java.util import Map
#from java.util import Set
#from java.util import TreeMap
#from java.util import UUID
#from org.I0Itec.zkclient import DataUpdater
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger
from org.apache.helix.AccessOption import AccessOption
from org.apache.helix.ConfigAccessor import ConfigAccessor
from org.apache.helix.ConfigScope import ConfigScope
from org.apache.helix.ConfigScope import ConfigScopeProperty
from org.apache.helix.HelixAdmin import HelixAdmin
from org.apache.helix.HelixConstants import HelixConstants
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixException import HelixException
from org.apache.helix.PropertyKey import PropertyKey
from org.apache.helix.PropertyKey import Builder
from org.apache.helix.PropertyPathConfig import PropertyPathConfig
from org.apache.helix.PropertyType import PropertyType
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.alerts.AlertsHolder import AlertsHolder
from org.apache.helix.alerts.StatsHolder import StatsHolder
from org.apache.helix.model.Alerts import Alerts
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.ExternalView import ExternalView
from org.apache.helix.model.IdealState import IdealState
from org.apache.helix.model.ClusterConstraints import ConstraintAttribute
from org.apache.helix.model.ClusterConstraints import ConstraintType
from org.apache.helix.model.IdealState import IdealStateModeProperty
from org.apache.helix.model.InstanceConfig import InstanceConfig
from org.apache.helix.model.InstanceConfig import InstanceConfigProperty
from org.apache.helix.model.LiveInstance import LiveInstance
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageState
from org.apache.helix.model.Message import MessageType
from org.apache.helix.model.PauseSignal import PauseSignal
from org.apache.helix.model.PersistentStats import PersistentStats
from org.apache.helix.model.StateModelDefinition import StateModelDefinition
from org.apache.helix.tools.IdealStateCalculatorForStorageNode import IdealStateCalculatorForStorageNode
from org.apache.helix.util.HelixUtil import HelixUtil


class ZKHelixAdmin(HelixAdmin):





    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = Logger.getLogger(ZKHelixAdmin.class)

    """

    Parameters:
        ZkClient zkClient
    """
    def __init__(self, zkClient):
        self._zkClient = zkClient
        self._configAccessor = ConfigAccessor(zkClient)


    def addInstance(self, clusterName, instanceConfig):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceConfig: InstanceConfig
        @Override


        """
        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # String
        instanceConfigsPath = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.PARTICIPANT.toString())
        # String
        nodeId = instanceConfig.getId()
        # String
        instanceConfigPath = instanceConfigsPath + "/" + nodeId
        if _zkClient.exists(instanceConfigPath): 
            raise HelixException("Node " + nodeId + " already exists in cluster " + clusterName)


        ZKUtil.createChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord())
        _zkClient.createPersistent(HelixUtil.getMessagePath(clusterName, nodeId), True)
        _zkClient.createPersistent(HelixUtil.getCurrentStateBasePath(clusterName, nodeId), True)
        _zkClient.createPersistent(HelixUtil.getErrorsPath(clusterName, nodeId), True)
        _zkClient.createPersistent(HelixUtil.getStatusUpdatesPath(clusterName, nodeId), True)


    def dropInstance(self, clusterName, instanceConfig):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceConfig: InstanceConfig
        @Override


        """
        # String
        instanceConfigsPath = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.PARTICIPANT.toString())
        # String
        nodeId = instanceConfig.getId()
        # String
        instanceConfigPath = instanceConfigsPath + "/" + nodeId
        # String
        instancePath = HelixUtil.getInstancePath(clusterName, nodeId)
        if not _zkClient.exists(instanceConfigPath): 
            raise HelixException("Node " + nodeId + " does not exist in config for cluster " + clusterName)


        if not _zkClient.exists(instancePath): 
            raise HelixException("Node " + nodeId + " does not exist in instances for cluster " + clusterName)


        ZKUtil.dropChildren(_zkClient, instanceConfigsPath, instanceConfig.getRecord())
        _zkClient.deleteRecursive(instancePath)


    def getInstanceConfig(self, clusterName, instanceName):
        """
        Returns InstanceConfig
        Parameters:
            clusterName: StringinstanceName: String
        @Override


        """
        # String
        instanceConfigPath = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.PARTICIPANT.toString(), instanceName)
        if not _zkClient.exists(instanceConfigPath): 
            raise HelixException("instance" + instanceName + " does not exist in cluster " + clusterName)


        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        return accessor.getProperty(keyBuilder.instanceConfig(instanceName))


    def enableInstance(self, clusterName, instanceName, enabled):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceName: Stringenabled: boolean
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.PARTICIPANT.toString(), instanceName)
        # ZkBaseDataAccessor<ZNRecord>
        baseAccessor = ZkBaseDataAccessor<ZNRecord>(_zkClient)
        if not baseAccessor.exists(path, 0): 
            raise HelixException("Cluster " + clusterName + ", instance: " + instanceName + ", instance config does not exist")


        baseAccessor.update(path, DataUpdater<ZNRecord>() {

            def update(self, currentData):
                """
                Returns ZNRecord
                Parameters:
                    currentData: ZNRecord
                @Override


                """
                if currentData == None: 
                    raise HelixException("Cluster: " + clusterName + ", instance: " + instanceName + ", participant config is null")


                # InstanceConfig
                config = InstanceConfig(currentData)
                config.setInstanceEnabled(enabled)
                return config.getRecord()

        }, AccessOption.PERSISTENT)


    def enablePartition(self, enabled, clusterName, instanceName, resourceName, partitionNames):
        """
        Returns void
        Parameters:
            enabled: booleanclusterName: StringinstanceName: StringresourceName: StringpartitionNames: List<String>
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.PARTICIPANT.toString(), instanceName)
        # ZkBaseDataAccessor<ZNRecord>
        baseAccessor = ZkBaseDataAccessor<ZNRecord>(_zkClient)
        if not baseAccessor.exists(path, 0): 
            raise HelixException("Cluster: " + clusterName + ", instance: " + instanceName + ", instance config does not exist")


        # String
        idealStatePath = PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName, resourceName)
        # ZNRecord
        idealStateRecord = None
        try:
            idealStateRecord = baseAccessor.get(idealStatePath, None, 0)
        except ZkNoNodeException, e:

        if idealStateRecord == None: 
            raise HelixException("Cluster: " + clusterName + ", resource: " + resourceName + ", ideal state does not exist")


        # IdealState
        idealState = IdealState(idealStateRecord)
        for # String
        partitionName = None
         in partitionNames) if (idealState.getIdealStateMode() == IdealStateModeProperty.AUTO && idealState.getPreferenceList(partitionName) == None) or (idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED && idealState.getInstanceStateMap(partitionName) == None): 
                logger.warn("Cluster: " + clusterName + ", resource: " + resourceName + ", partition: " + partitionName + ", partition does not exist in ideal state")


        baseAccessor.update(path, DataUpdater<ZNRecord>() {

            def update(self, currentData):
                """
                Returns ZNRecord
                Parameters:
                    currentData: ZNRecord
                @Override


                """
                if currentData == None: 
                    raise HelixException("Cluster: " + clusterName + ", instance: " + instanceName + ", participant config is null")


                # List<String>
                list = currentData.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString())
                # Set<String>
                disabledPartitions = HashSet<String>()
                if list != None: 
                    disabledPartitions.addAll(list)

                if enabled: 
                    disabledPartitions.removeAll(partitionNames)
                else:
                    disabledPartitions.addAll(partitionNames)

                list = ArrayList<String>(disabledPartitions)
                Collections.sort(list)
                currentData.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(), list)
                return currentData

        }, AccessOption.PERSISTENT)


    def enableCluster(self, clusterName, enabled):
        """
        Returns void
        Parameters:
            clusterName: Stringenabled: boolean
        @Override


        """
        # HelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        if enabled: 
            accessor.removeProperty(keyBuilder.pause())
        else:
            accessor.createProperty(keyBuilder.pause(), PauseSignal("pause"))



    def resetPartition(self, clusterName, instanceName, resourceName, partitionNames):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceName: StringresourceName: StringpartitionNames: List<String>
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        # LiveInstance
        liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName))
        if liveInstance == None: 
            raise HelixException("Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName + ", because " + instanceName + " is not alive")


        # IdealState
        idealState = accessor.getProperty(keyBuilder.idealStates(resourceName))
        if idealState == None: 
            raise HelixException("Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName + ", because " + resourceName + " is not added")


        # Set<String>
        resetPartitionNames = HashSet<String>(partitionNames)
        if idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED: 
            # Set<String>
            partitions = HashSet<String>(idealState.getRecord().getMapFields().keySet())
            if not partitions.containsAll(resetPartitionNames): 
                raise HelixException("Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName + ", because not all " + partitionNames + " exist")


        else:
            # Set<String>
            partitions = HashSet<String>(idealState.getRecord().getListFields().keySet())
            if not partitions.containsAll(resetPartitionNames): 
                raise HelixException("Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName + ", because not all " + partitionNames + " exist")



        # String
        sessionId = liveInstance.getSessionId()
        # CurrentState
        curState = accessor.getProperty(keyBuilder.currentState(instanceName, sessionId, resourceName))
        for # String
        partitionName = None
         in resetPartitionNames) if not (curState.getState(partitionName) == "ERROR"): 
                raise HelixException("Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName + ", because not all " + partitionNames + " are in ERROR state")



        # String
        stateModelDef = idealState.getStateModelDefRef()
        # StateModelDefinition
        stateModel = accessor.getProperty(keyBuilder.stateModelDef(stateModelDef))
        if stateModel == None: 
            raise HelixException("Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName + ", because " + stateModelDef + " is NOT found")


        # List<Message>
        messages = accessor.getChildValues(keyBuilder.messages(instanceName))
        for # Message
        message = None
         in messages) if not MessageType.STATE_TRANSITION.toString().equalsIgnoreCase(message.getMsgType()) or not (sessionId == message.getTgtSessionId()) or not (resourceName == message.getResourceName()) or not resetPartitionNames.contains(message.getPartitionName()): 
                continue

            raise HelixException("Can't reset state for " + resourceName + "/" + partitionNames + " on " + instanceName + ", because a pending message exists: " + message)


        # String
        adminName = None
        try:
            adminName = InetAddress.getLocalHost().getCanonicalHostName() + "-ADMIN"
        except UnknownHostException, e:
            logger.info("Unable to get host name. Will set it to UNKNOWN, mostly ignorable", e)
            adminName = "UNKNOWN"

        # List<Message>
        resetMessages = ArrayList<Message>()
        # List<PropertyKey>
        messageKeys = ArrayList<PropertyKey>()
        for # String
        partitionName = None
         in resetPartitionNames) # String
            msgId = UUID.randomUUID().toString()
            # Message
            message = Message(MessageType.STATE_TRANSITION, msgId)
            message.setSrcName(adminName)
            message.setTgtName(instanceName)
            message.setMsgState(MessageState.NEW)
            message.setPartitionName(partitionName)
            message.setResourceName(resourceName)
            message.setTgtSessionId(sessionId)
            message.setStateModelDef(stateModelDef)
            message.setFromState("ERROR")
            message.setToState(stateModel.getInitialState())
            message.setStateModelFactoryName(idealState.getStateModelFactoryName())
            resetMessages.add(message)
            messageKeys.add(keyBuilder.message(instanceName, message.getId()))

        accessor.setChildren(messageKeys, resetMessages)


    def resetInstance(self, clusterName, instanceNames):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceNames: List<String>
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        # List<ExternalView>
        extViews = accessor.getChildValues(keyBuilder.externalViews())
        # Set<String>
        resetInstanceNames = HashSet<String>(instanceNames)
        for # String
        instanceName = None
         in resetInstanceNames) # List<String>
            resetPartitionNames = ArrayList<String>()
            for # ExternalView
            extView = None
             in extViews) # Map<String, Map<String, String>>
                stateMap = extView.getRecord().getMapFields()
                for # String
                partitionName = None
                 in stateMap.keySet()) # Map<String, String>
                    instanceStateMap = stateMap.get(partitionName)
                    if instanceStateMap.containsKey(instanceName) && (instanceStateMap.get(instanceName) == "ERROR"): 
                        resetPartitionNames.add(partitionName)


                resetPartition(clusterName, instanceName, extView.getResourceName(), resetPartitionNames)




    def resetResource(self, clusterName, resourceNames):
        """
        Returns void
        Parameters:
            clusterName: StringresourceNames: List<String>
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        # List<ExternalView>
        extViews = accessor.getChildValues(keyBuilder.externalViews())
        # Set<String>
        resetResourceNames = HashSet<String>(resourceNames)
        for # ExternalView
        extView = None
         in extViews) if not resetResourceNames.contains(extView.getResourceName()): 
                continue

            # Map<String, List<String>>
            resetPartitionNames = HashMap<String, List<String>>()
            # Map<String, Map<String, String>>
            stateMap = extView.getRecord().getMapFields()
            for # String
            partitionName = None
             in stateMap.keySet()) # Map<String, String>
                instanceStateMap = stateMap.get(partitionName)
                for # String
                instanceName = None
                 in instanceStateMap.keySet()) if (instanceStateMap.get(instanceName) == "ERROR"): 
                        if not resetPartitionNames.containsKey(instanceName): 
                            resetPartitionNames.put(instanceName, ArrayList<String>())

                        resetPartitionNames.get(instanceName).add(partitionName)



            for # String
            instanceName = None
             in resetPartitionNames.keySet()) resetPartition(clusterName, instanceName, extView.getResourceName(), resetPartitionNames.get(instanceName))




    def addCluster(self, clusterName, overwritePrevRecord):
        """
        Returns void
        Parameters:
            clusterName: StringoverwritePrevRecord: boolean
        @Override


        """
        # String
        root = "/" + clusterName
        # String
        path = None
        
        if _zkClient.exists(root): 
            logger.warn("Root directory exists.Cleaning the root directory:" + root + " overwritePrevRecord: " + overwritePrevRecord)
            if overwritePrevRecord: 
                _zkClient.deleteRecursive(root)
            else:
                raise HelixException("Cluster " + clusterName + " already exists")



        _zkClient.createPersistent(root)
        _zkClient.createPersistent(HelixUtil.getIdealStatePath(clusterName))
        path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.CLUSTER.toString(), clusterName)
        _zkClient.createPersistent(path, True)
        _zkClient.writeData(path, ZNRecord(clusterName))
        path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.PARTICIPANT.toString())
        _zkClient.createPersistent(path)
        path = PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.RESOURCE.toString())
        _zkClient.createPersistent(path)
        path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName)
        _zkClient.createPersistent(path)
        _zkClient.createPersistent(HelixUtil.getLiveInstancesPath(clusterName))
        _zkClient.createPersistent(HelixUtil.getMemberInstancesPath(clusterName))
        _zkClient.createPersistent(HelixUtil.getExternalViewPath(clusterName))
        _zkClient.createPersistent(HelixUtil.getStateModelDefinitionPath(clusterName))
        _zkClient.createPersistent(HelixUtil.getControllerPath(clusterName))
        path = PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName)
        # ZNRecord
        emptyHistory = ZNRecord(PropertyType.HISTORY.toString())
        # List<String>
        emptyList = ArrayList<String>()
        emptyHistory.setListField(clusterName, emptyList)
        _zkClient.createPersistent(path, emptyHistory)
        path = PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, clusterName)
        _zkClient.createPersistent(path)
        path = PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER, clusterName)
        _zkClient.createPersistent(path)
        path = PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER, clusterName)
        _zkClient.createPersistent(path)


    def getInstancesInCluster(self, clusterName):
        """
        Returns List<String>
        Parameters:
            clusterName: String
        @Override


        """
        # String
        memberInstancesPath = HelixUtil.getMemberInstancesPath(clusterName)
        return _zkClient.getChildren(memberInstancesPath)


    def addResource(self, clusterName, resourceName, partitions, stateModelRef):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: Stringpartitions: intstateModelRef: String
        @Override


        """
        addResource(clusterName, resourceName, partitions, stateModelRef, IdealStateModeProperty.AUTO.toString(), 0)


    def addResource(self, clusterName, resourceName, partitions, stateModelRef, idealStateMode):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: Stringpartitions: intstateModelRef: StringidealStateMode: String
        @Override


        """
        addResource(clusterName, resourceName, partitions, stateModelRef, idealStateMode, 0)


    def addResource(self, clusterName, resourceName, partitions, stateModelRef, idealStateMode, bucketSize):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: Stringpartitions: intstateModelRef: StringidealStateMode: StringbucketSize: int
        @Override


        """
        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # IdealStateModeProperty
        mode = IdealStateModeProperty.AUTO
        try:
            mode = IdealStateModeProperty.valueOf(idealStateMode)
        except Exception, e:
            logger.error(""+ str(e))

        # IdealState
        idealState = IdealState(resourceName)
        idealState.setNumPartitions(partitions)
        idealState.setStateModelDefRef(stateModelRef)
        idealState.setIdealStateMode(mode.toString())
        idealState.setReplicas("" + 0)
        idealState.setStateModelFactoryName(HelixConstants.DEFAULT_STATE_MODEL_FACTORY)
        if bucketSize > 0: 
            idealState.setBucketSize(bucketSize)

        # String
        stateModelDefPath = PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName, stateModelRef)
        if not _zkClient.exists(stateModelDefPath): 
            raise HelixException("State model " + stateModelRef + " not found in the cluster STATEMODELDEFS path")


        # String
        idealStatePath = HelixUtil.getIdealStatePath(clusterName)
        # String
        dbIdealStatePath = idealStatePath + "/" + resourceName
        if _zkClient.exists(dbIdealStatePath): 
            raise HelixException("Skip the operation. DB ideal state directory exists:" + dbIdealStatePath)


        ZKUtil.createChildren(_zkClient, idealStatePath, idealState.getRecord())


    def getClusters(self):
        """
        Returns List<String>
        @Override


        """
        # List<String>
        zkToplevelPathes = _zkClient.getChildren("/")
        # List<String>
        result = ArrayList<String>()
        for # String
        pathName = None
         in zkToplevelPathes) if ZKUtil.isClusterSetup(pathName, _zkClient): 
                result.add(pathName)


        return result


    def getResourcesInCluster(self, clusterName):
        """
        Returns List<String>
        Parameters:
            clusterName: String
        @Override


        """
        return _zkClient.getChildren(HelixUtil.getIdealStatePath(clusterName))


    def getResourceIdealState(self, clusterName, dbName):
        """
        Returns IdealState
        Parameters:
            clusterName: StringdbName: String
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        return accessor.getProperty(keyBuilder.idealStates(dbName))


    def setResourceIdealState(self, clusterName, dbName, idealState):
        """
        Returns void
        Parameters:
            clusterName: StringdbName: StringidealState: IdealState
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        accessor.setProperty(keyBuilder.idealStates(dbName), idealState)


    def getResourceExternalView(self, clusterName, resourceName):
        """
        Returns ExternalView
        Parameters:
            clusterName: StringresourceName: String
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        return accessor.getProperty(keyBuilder.externalView(resourceName))


    def addStateModelDef(self, clusterName, stateModelDef, stateModel):
        """
        Returns void
        Parameters:
            clusterName: StringstateModelDef: StringstateModel: StateModelDefinition
        @Override


        """
        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # String
        stateModelDefPath = HelixUtil.getStateModelDefinitionPath(clusterName)
        # String
        stateModelPath = stateModelDefPath + "/" + stateModelDef
        if _zkClient.exists(stateModelPath): 
            logger.warn("Skip the operation.State Model directory exists:" + stateModelPath)
            raise HelixException("State model path " + stateModelPath + " already exists.")


        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        accessor.setProperty(keyBuilder.stateModelDef(stateModel.getId()), stateModel)


    def dropResource(self, clusterName, resourceName):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: String
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        accessor.removeProperty(keyBuilder.idealStates(resourceName))


    def getStateModelDefs(self, clusterName):
        """
        Returns List<String>
        Parameters:
            clusterName: String
        @Override


        """
        return _zkClient.getChildren(HelixUtil.getStateModelDefinitionPath(clusterName))


    def getStateModelDef(self, clusterName, stateModelName):
        """
        Returns StateModelDefinition
        Parameters:
            clusterName: StringstateModelName: String
        @Override


        """
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        return accessor.getProperty(keyBuilder.stateModelDef(stateModelName))


    def addStat(self, clusterName, statName):
        """
        Returns void
        Parameters:
            clusterName: StringstatName: String
        @Override


        """
        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # String
        persistentStatsPath = PropertyPathConfig.getPath(PropertyType.PERSISTENTSTATS, clusterName)
        # ZkBaseDataAccessor<ZNRecord>
        baseAccessor = ZkBaseDataAccessor<ZNRecord>(_zkClient)
        baseAccessor.update(persistentStatsPath, DataUpdater<ZNRecord>() {

            def update(self, statsRec):
                """
                Returns ZNRecord
                Parameters:
                    statsRec: ZNRecord
                @Override


                """
                if statsRec == None: 
                    statsRec = ZNRecord(PersistentStats.nodeName)

                # Map<String, Map<String, String>>
                currStatMap = statsRec.getMapFields()
                # Map<String, Map<String, String>>
                newStatMap = StatsHolder.parseStat(statName)
                for # String
                newStat = None
                 in newStatMap.keySet()) if not currStatMap.containsKey(newStat): 
                        currStatMap.put(newStat, newStatMap.get(newStat))


                statsRec.setMapFields(currStatMap)
                return statsRec

        }, AccessOption.PERSISTENT)


    def addAlert(self, clusterName, alertName):
        """
        Returns void
        Parameters:
            clusterName: StringalertName: String
        @Override


        """
        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # ZkBaseDataAccessor<ZNRecord>
        baseAccessor = ZkBaseDataAccessor<ZNRecord>(_zkClient)
        # String
        alertsPath = PropertyPathConfig.getPath(PropertyType.ALERTS, clusterName)
        baseAccessor.update(alertsPath, DataUpdater<ZNRecord>() {

            def update(self, alertsRec):
                """
                Returns ZNRecord
                Parameters:
                    alertsRec: ZNRecord
                @Override


                """
                if alertsRec == None: 
                    alertsRec = ZNRecord(Alerts.nodeName)

                # Map<String, Map<String, String>>
                currAlertMap = alertsRec.getMapFields()
                # StringBuilder
                newStatName = StringBuilder()
                # Map<String, String>
                newAlertMap = HashMap<String, String>()
                AlertsHolder.parseAlert(alertName, newStatName, newAlertMap)
                addStat(clusterName, newStatName.toString())
                currAlertMap.put(alertName, newAlertMap)
                alertsRec.setMapFields(currAlertMap)
                return alertsRec

        }, AccessOption.PERSISTENT)


    def dropCluster(self, clusterName):
        """
        Returns void
        Parameters:
            clusterName: String
        @Override


        """
        logger.info("Deleting cluster " + clusterName)
        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(clusterName, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        # String
        root = "/" + clusterName
        if accessor.getChildNames(keyBuilder.liveInstances()).size() > 0: 
            raise HelixException("There are still live instances in the cluster, shut them down first.")


        if accessor.getProperty(keyBuilder.controllerLeader()) != None: 
            raise HelixException("There are still LEADER in the cluster, shut them down first.")


        _zkClient.deleteRecursive(root)


    def dropStat(self, clusterName, statName):
        """
        Returns void
        Parameters:
            clusterName: StringstatName: String
        @Override


        """
        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # String
        persistentStatsPath = PropertyPathConfig.getPath(PropertyType.PERSISTENTSTATS, clusterName)
        # ZkBaseDataAccessor<ZNRecord>
        baseAccessor = ZkBaseDataAccessor<ZNRecord>(_zkClient)
        baseAccessor.update(persistentStatsPath, DataUpdater<ZNRecord>() {

            def update(self, statsRec):
                """
                Returns ZNRecord
                Parameters:
                    statsRec: ZNRecord
                @Override


                """
                if statsRec == None: 
                    raise HelixException("No stats record in ZK, nothing to drop")


                # Map<String, Map<String, String>>
                currStatMap = statsRec.getMapFields()
                # Map<String, Map<String, String>>
                newStatMap = StatsHolder.parseStat(statName)
                for # String
                newStat = None
                 in newStatMap.keySet()) if currStatMap.containsKey(newStat): 
                        currStatMap.remove(newStat)


                statsRec.setMapFields(currStatMap)
                return statsRec

        }, AccessOption.PERSISTENT)


    def dropAlert(self, clusterName, alertName):
        """
        Returns void
        Parameters:
            clusterName: StringalertName: String
        @Override


        """
        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # String
        alertsPath = PropertyPathConfig.getPath(PropertyType.ALERTS, clusterName)
        # ZkBaseDataAccessor<ZNRecord>
        baseAccessor = ZkBaseDataAccessor<ZNRecord>(_zkClient)
        if not baseAccessor.exists(alertsPath, 0): 
            raise HelixException("No alerts node in ZK, nothing to drop")


        baseAccessor.update(alertsPath, DataUpdater<ZNRecord>() {

            def update(self, alertsRec):
                """
                Returns ZNRecord
                Parameters:
                    alertsRec: ZNRecord
                @Override


                """
                if alertsRec == None: 
                    raise HelixException("No alerts record in ZK, nothing to drop")


                # Map<String, Map<String, String>>
                currAlertMap = alertsRec.getMapFields()
                currAlertMap.remove(alertName)
                alertsRec.setMapFields(currAlertMap)
                return alertsRec

        }, AccessOption.PERSISTENT)


    def addClusterToGrandCluster(self, clusterName, grandCluster):
        """
        Returns void
        Parameters:
            clusterName: StringgrandCluster: String
        @Override


        """
        if not ZKUtil.isClusterSetup(grandCluster, _zkClient): 
            raise HelixException("Grand cluster " + grandCluster + " is not setup yet")


        if not ZKUtil.isClusterSetup(clusterName, _zkClient): 
            raise HelixException("Cluster " + clusterName + " is not setup yet")


        # IdealState
        idealState = IdealState(clusterName)
        idealState.setNumPartitions(1)
        idealState.setStateModelDefRef("LeaderStandby")
        # List<String>
        controllers = getInstancesInCluster(grandCluster)
        if controllers.size() == 0: 
            raise HelixException("Grand cluster " + grandCluster + " has no instances")


        idealState.setReplicas(Integer.toString(controllers.size()))
        Collections.shuffle(controllers)
        idealState.getRecord().setListField(clusterName, controllers)
        idealState.setPartitionState(clusterName, controllers.get(0), "LEADER")
        for (# int
        i = 1; i < controllers.size(); i++) idealState.setPartitionState(clusterName, controllers.get(i), "STANDBY")

        # ZKHelixDataAccessor
        accessor = ZKHelixDataAccessor(grandCluster, ZkBaseDataAccessor<ZNRecord>(_zkClient))
        # Builder
        keyBuilder = accessor.keyBuilder()
        accessor.setProperty(keyBuilder.idealStates(idealState.getResourceName()), idealState)


    def setConfig(self, scope, properties):
        """
        Returns void
        Parameters:
            scope: ConfigScopeproperties: Map<String, String>
        @Override


        """
        for # String
        key = None
         in properties.keySet()) _configAccessor.set(scope, key, properties.get(key))



    def getConfig(self, scope, keys):
        """
        Returns Map<String, String>
        Parameters:
            scope: ConfigScopekeys: Set<String>
        @Override


        """
        # Map<String, String>
        properties = TreeMap<String, String>()
        if keys == None: 
        else:
            for # String
            key = None
             in keys) # String
                value = _configAccessor.get(scope, key)
                if value == None: 
                    logger.error("Config doesn't exist for key: " + str(key))
                    continue

                properties.put(key, value)


        return properties


    def getConfigKeys(self, scope, clusterName, keys):
        """
        Returns List<String>
        Parameters:
            scope: ConfigScopePropertyclusterName: Stringkeys: String
        @Override


        """
        return _configAccessor.getKeys(scope, clusterName, keys)


    def removeConfig(self, scope, keys):
        """
        Returns void
        Parameters:
            scope: ConfigScopekeys: Set<String>
        @Override


        """
        for # String
        key = None
         in keys) _configAccessor.remove(scope, key)



    def rebalance(self, clusterName, resourceName, replica):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: Stringreplica: int
        @Override


        """
        rebalance(clusterName, resourceName, replica, resourceName)


    def rebalance(self, clusterName, resourceName, replica, keyPrefix):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: Stringreplica: intkeyPrefix: String


        """
        # List<String>
        InstanceNames = getInstancesInCluster(clusterName)
        Collections.sort(InstanceNames)
        # IdealState
        idealState = getResourceIdealState(clusterName, resourceName)
        if idealState == None: 
            raise HelixException("Resource: " + resourceName + " has NOT been added yet")


        idealState.setReplicas(Integer.toString(replica))
        # int
        partitions = idealState.getNumPartitions()
        # String
        stateModelName = idealState.getStateModelDefRef()
        # StateModelDefinition
        stateModDef = getStateModelDef(clusterName, stateModelName)
        if stateModDef == None: 
            raise HelixException("cannot find state model: " + stateModelName)


        # List<String>
        statePriorityList = stateModDef.getStatesPriorityList()
        # String
        masterStateValue = None
        # String
        slaveStateValue = None
        replica--
        for # String
        state = None
         in statePriorityList) # String
            count = stateModDef.getNumInstancesPerState(state)
            if (count == "1"): 
                if masterStateValue != None: 
                    raise HelixException("Invalid or unsupported state model definition")


                masterStateValue = state
            else:
                if count.equalsIgnoreCase("R"): 
                    if slaveStateValue != None: 
                        raise HelixException("Invalid or unsupported state model definition")


                    slaveStateValue = state
                else:
                    if count.equalsIgnoreCase("N"): 
                        if not (masterStateValue == None && slaveStateValue == None): 
                            raise HelixException("Invalid or unsupported state model definition")


                        replica = InstanceNames.size() - 1
                        masterStateValue = slaveStateValue = state


        if masterStateValue == None && slaveStateValue == None: 
            raise HelixException("Invalid or unsupported state model definition")


        if masterStateValue == None: 
            masterStateValue = slaveStateValue

        if idealState.getIdealStateMode() != IdealStateModeProperty.AUTO_REBALANCE: 
            # ZNRecord
            newIdealState = IdealStateCalculatorForStorageNode.calculateIdealState(InstanceNames, partitions, replica, keyPrefix, masterStateValue, slaveStateValue)
            if idealState.getIdealStateMode() == IdealStateModeProperty.AUTO: 
                idealState.getRecord().setListFields(newIdealState.getListFields())
                idealState.getRecord().setMapFields(newIdealState.getMapFields())

            if idealState.getIdealStateMode() == IdealStateModeProperty.CUSTOMIZED: 
                idealState.getRecord().setMapFields(newIdealState.getMapFields())

        else:
            for (# int
            i = 0; i < partitions; i++) # String
                partitionName = keyPrefix + "_" + i
                idealState.getRecord().setMapField(partitionName, HashMap<String, String>())
                idealState.getRecord().setListField(partitionName, ArrayList<String>())


        setResourceIdealState(clusterName, resourceName, idealState)


    def addIdealState(self, clusterName, resourceName, idealStateFile):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: StringidealStateFile: String
        @Override


        Throws: 
            IOException
        """
        # ZNRecord
        idealStateRecord = (ZNRecord) (ZNRecordSerializer().deserialize(readFile(idealStateFile)))
        if idealStateRecord.getId() == None or not (idealStateRecord.getId() == resourceName): 
            raise IllegalArgumentException("ideal state must have same id as resource name")


        setResourceIdealState(clusterName, resourceName, IdealState(idealStateRecord))


    def readFile(filePath):
        """
        Returns byte[]
        Parameters:
            filePath: String
        Java modifiers:
             private static

        Throws: 
            IOException
        """
        # File
        file = File(filePath)
        # int
        size = (int) file.length()
        # byte[]
        bytes = new byte[size]
        # DataInputStream
        dis = DataInputStream(FileInputStream(file))
        # int
        read = 0
        # int
        numRead = 0
        while (read < bytes.length && (numRead = dis.read(bytes, read, bytes.length - read)) >= 0:
            read = read + numRead

        return bytes


    def addStateModelDef(self, clusterName, stateModelDefName, stateModelDefFile):
        """
        Returns void
        Parameters:
            clusterName: StringstateModelDefName: StringstateModelDefFile: String


        Throws: 
            IOException
        """
        # ZNRecord
        record = (ZNRecord) (ZNRecordSerializer().deserialize(readFile(stateModelDefFile)))
        if record == None or record.getId() == None or not (record.getId() == stateModelDefName): 
            raise IllegalArgumentException("state model definition must have same id as state model def name")


        addStateModelDef(clusterName, stateModelDefName, StateModelDefinition(record))


    def addMessageConstraint(self, clusterName, constraintId, constraints):
        """
        Returns void
        Parameters:
            clusterName: StringconstraintId: Stringconstraints: Map<String, String>


        """
        # ZkBaseDataAccessor<ZNRecord>
        baseAccessor = ZkBaseDataAccessor<ZNRecord>(_zkClient)
        # Builder
        keyBuilder = Builder(clusterName)
        # String
        path = keyBuilder.constraint(ConstraintType.MESSAGE_CONSTRAINT.toString()).getPath()
        baseAccessor.update(path, DataUpdater<ZNRecord>() {

            def update(self, currentData):
                """
                Returns ZNRecord
                Parameters:
                    currentData: ZNRecord
                @Override


                """
                if currentData == None: 
                    currentData = ZNRecord(ConstraintType.MESSAGE_CONSTRAINT.toString())

                # Map<String, String>
                map = currentData.getMapField(constraintId)
                if map == None: 
                    map = TreeMap<String, String>()
                    currentData.setMapField(constraintId, map)
                else:
                    logger.warn("Overwrite existing constraint " + constraintId + ": " + map)

                for # String
                key = None
                 in constraints.keySet()) # ConstraintAttribute
                    attr = ConstraintAttribute.valueOf(key.toUpperCase())
                    map.put(attr.toString(), constraints.get(key))

                return currentData

        }, AccessOption.PERSISTENT)



