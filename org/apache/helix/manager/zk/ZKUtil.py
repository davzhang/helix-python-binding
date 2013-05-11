# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util import ArrayList
#from java.util import Collections
#from java.util import List
#from org.I0Itec.zkclient import DataUpdater
#from org.apache.log4j import Logger
#from org.apache.zookeeper import CreateMode
#from org.apache.zookeeper.data import Stat
from ImageStat import Stat
from org.apache.helix.ConfigScope import ConfigScopeProperty
from org.apache.helix.PropertyPathConfig import PropertyPathConfig
from org.apache.helix.PropertyType import PropertyType
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.util.ZKConstants import HelixZNodeStat


from org.apache.helix.util.logger import get_logger
class ZKUtil:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """
    Java modifiers:
         private static
    Type:
        int
    """
    RETRYLIMIT = 3

    """
    Java modifiers:
         private
    """
    def __init__(self): pass

    @staticmethod
    def isClusterSetup(clusterName, zkClient):
        """
        Returns boolean
        Parameters:
            clusterName: StringzkClient: ZkClient
        Java modifiers:
             static

        """
        if clusterName == None or zkClient == None:
            return False

        # boolean
        isValid = zkClient.exists(PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.CLUSTER), clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.PARTICIPANT))) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName, ConfigScopeProperty.toString(ConfigScopeProperty.RESOURCE))) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName))  \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.LIVEINSTANCES, clusterName))  \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.INSTANCES, clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.EXTERNALVIEW, clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.CONTROLLER, clusterName))  \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.STATEMODELDEFS, clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.MESSAGES_CONTROLLER, clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.ERRORS_CONTROLLER, clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.STATUSUPDATES_CONTROLLER, clusterName)) \
            and  zkClient.exists(PropertyPathConfig.getPath(PropertyType.HISTORY, clusterName))
        return isValid

    @staticmethod
    def createOrUpdate(client, path, record, persistent, mergeOnUpdate):
        """
        Returns void
        Parameters:
            client: ZkClientpath: Stringrecord: ZNRecordpersistent: booleanmergeOnUpdate: boolean
        Java modifiers:
             static

        """
        # int
        retryCount = 0
        while retryCount < ZKUtil.RETRYLIMIT:
            try:
                if not client.exists(path):
                    client.createPersistent(path, True)
                stat = HelixZNodeStat()
                currentRecord = client.readDataAndStat(path, stat, True)
                if currentRecord is not None and mergeOnUpdate:
                    currentRecord.merge(record)
                client.writeData(path, currentRecord, stat.getVersion())
                break
            except Exception as e:
                retryCount = retryCount + 1
                ZKUtil.logger.warn("Exception trying to update " + path + " Exception:" + str(e) + ". Will retry.")


#    def createChildren(client, parentPath, list):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientparentPath: Stringlist: List<ZNRecord>
#        Java modifiers:
#             static
#
#        """
#        client.createPersistent(parentPath, True)
#        if list != None:
#            for # ZNRecord
#            record = None
#             in list) createChildren(client, parentPath, record)
#
#
#
#
#    def createChildren(client, parentPath, nodeRecord):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientparentPath: StringnodeRecord: ZNRecord
#        Java modifiers:
#             static
#
#        """
#        client.createPersistent(parentPath, True)
#        # String
#        id = nodeRecord.getId()
#        # String
#        temp = parentPath + "/" + id
#        client.createPersistent(temp, nodeRecord)
#
#
#    def dropChildren(client, parentPath, list):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientparentPath: Stringlist: List<ZNRecord>
#        Java modifiers:
#             static
#
#        """
#        if list != None:
#            for # ZNRecord
#            record = None
#             in list) dropChildren(client, parentPath, record)
#
#
#
#
#    def dropChildren(client, parentPath, nodeRecord):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientparentPath: StringnodeRecord: ZNRecord
#        Java modifiers:
#             static
#
#        """
#        # String
#        id = nodeRecord.getId()
#        # String
#        temp = parentPath + "/" + id
#        client.deleteRecursive(temp)
#
#
#    def getChildren(client, path):
#        """
#        Returns List<ZNRecord>
#        Parameters:
#            client: ZkClientpath: String
#        Java modifiers:
#             static
#
#        """
#        # List<String>
#        children = client.getChildren(path)
#        if children == None or children.size() == 0:
#            return Collections.emptyList()
#
#        # List<ZNRecord>
#        childRecords = ArrayList<ZNRecord>()
#        for # String
#        child = None
#         in children) # String
#            childPath = path + "/" + child
#            # Stat
#            newStat = Stat()
#            # ZNRecord
#            record = client.readDataAndStat(childPath, newStat, True)
#            if record != None:
#                record.setVersion(newStat.getVersion())
#                record.setCreationTime(newStat.getCtime())
#                record.setModifiedTime(newStat.getMtime())
#                childRecords.add(record)
#
#
#        return childRecords
#
#
#    def updateIfExists(client, path, record, mergeOnUpdate):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientpath: Stringrecord: ZNRecordmergeOnUpdate: boolean
#        Java modifiers:
#             static
#
#        """
#        if client.exists(path):
#            # DataUpdater<Object>
#            updater = DataUpdater<Object>() {
#
#                def update(self, currentData):
#                    """
#                    Returns Object
#                    Parameters:
#                        currentData: Object
#                    @Override
#
#
#                    """
#                    return record
#
#            }
#            client.updateDataSerialized(path, updater)
#
#
#
#
#
#
#
#    def asyncCreateOrUpdate(client, path, record, persistent, mergeOnUpdate):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientpath: Stringrecord: ZNRecordpersistent: booleanmergeOnUpdate: boolean
#        Java modifiers:
#             static
#
#        """
#        try:
#            if client.exists(path):
#                if mergeOnUpdate:
#                    # ZNRecord
#                    curRecord = client.readData(path)
#                    if curRecord != None:
#                        curRecord.merge(record)
#                        client.asyncSetData(path, curRecord, -1, None)
#                    else:
#                        client.asyncSetData(path, record, -1, None)
#
#                else:
#                    client.asyncSetData(path, record, -1, None)
#
#            else:
#                # CreateMode
#                mode = java2python_runtime.ternary((persistent), CreateMode.PERSISTENT, CreateMode.EPHEMERAL)
#                if record.getDeltaList().size() > 0:
#                    # ZNRecord
#                    newRecord = ZNRecord(record.getId())
#                    newRecord.merge(record)
#                    client.create(path, None, mode)
#                    client.asyncSetData(path, newRecord, -1, None)
#                else:
#                    client.create(path, None, mode)
#                    client.asyncSetData(path, record, -1, None)
#
#
#        except Exception, e:
#            logger.error("Exception in async create or update " + str(path)+ ". Exception: " + str(e.getMessage())+ ". Give up.")
#
#
#
#    def createOrReplace(client, path, record, persistent):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientpath: Stringrecord: ZNRecordpersistent: boolean
#        Java modifiers:
#             static
#
#        """
#        # int
#        retryCount = 0
#        while (retryCount < RETRYLIMIT:
#            try:
#                if client.exists(path):
#                    # DataUpdater<Object>
#                    updater = DataUpdater<Object>() {
#
#                        def update(self, currentData):
#                            """
#                            Returns Object
#                            Parameters:
#                                currentData: Object
#                            @Override
#
#
#                            """
#                            return record
#
#                    }
#                    client.updateDataSerialized(path, updater)
#                else:
#                    # CreateMode
#                    mode = java2python_runtime.ternary((persistent), CreateMode.PERSISTENT, CreateMode.EPHEMERAL)
#                    client.create(path, record, mode)
#
#                break
#            except Exception, e:
#                retryCount = retryCount + 1
#                logger.warn("Exception trying to createOrReplace " + path + " Exception:" + e.getMessage() + ". Will retry.")
#
#
#
#
#    def subtract(client, path, recordTosubtract):
#        """
#        Returns void
#        Parameters:
#            client: ZkClientpath: StringrecordTosubtract: ZNRecord
#        Java modifiers:
#             static
#
#        """
#        # int
#        retryCount = 0
#        while (retryCount < RETRYLIMIT:
#            try:
#                if client.exists(path):
#                    # DataUpdater<ZNRecord>
#                    updater = DataUpdater<ZNRecord>() {
#
#                        def update(self, currentData):
#                            """
#                            Returns ZNRecord
#                            Parameters:
#                                currentData: ZNRecord
#                            @Override
#
#
#                            """
#                            currentData.subtract(recordTosubtract)
#                            return currentData
#
#                    }
#                    client.updateDataSerialized(path, updater)
#                    break
#
#            except Exception, e:
#                retryCount = retryCount + 1
#                logger.warn("Exception trying to createOrReplace " + path + " Exception:" + e.getMessage() + ". Will retry.")
#                e.printStackTrace()
#
#
#
#
#
