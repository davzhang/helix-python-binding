# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util import ArrayList
#from java.util import Collections
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from org.I0Itec.zkclient import DataUpdater
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger
#from org.apache.zookeeper.data import Stat
from kazoo.protocol.states import ZnodeStat
from org.apache.helix.AccessOption import AccessOption
from org.apache.helix.BaseDataAccessor import BaseDataAccessor
from org.apache.helix.ControllerChangeListener import ControllerChangeListener
from org.apache.helix.GroupCommit import GroupCommit
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.PropertyKey import PropertyKey
#from org.apache.helix.PropertyKey import Builder
from org.apache.helix.PropertyType import PropertyType
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.ZNRecordAssembler import ZNRecordAssembler
#from org.apache.helix.ZNRecordBucketizer import ZNRecordBucketizer
from org.apache.helix.ZNRecordUpdater import ZNRecordUpdater
#from org.apache.helix.controller.restlet.ZNRecordUpdate import ZNRecordUpdate
#from org.apache.helix.controller.restlet.ZNRecordUpdate import OpCode
#from org.apache.helix.controller.restlet.ZkPropertyTransferClient import ZkPropertyTransferClient
from org.apache.helix.model.LiveInstance import LiveInstance
from org.apache.helix.util.ZKConstants import HelixZNodeStat

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.UserExceptions import IllegalArgumentException

from org.apache.helix.PropertyType import *
#import zookeeper
import kazoo.exceptions
#from org.apache.helix.PropertyType import PropertyType

class ZKHelixDataAccessor(HelixDataAccessor, ControllerChangeListener):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
#    LOG = Logger.getLogger(ZKHelixDataAccessor.class)
    LOG = get_logger(__name__)


    """

    Parameters:
        String clusterName
        InstanceType instanceType
        BaseDataAccessor<ZNRecord> baseDataAccessor
    """
    def __init__(self, clusterName, instanceType=None, baseDataAccessor=None):
        self._clusterName = clusterName
        self._instanceType = instanceType
        self._baseDataAccessor = baseDataAccessor
        self._propertyKeyBuilder = PropertyKey.Builder(self._clusterName)
        self._zkPropertyTransferClient = None
        self._groupCommit = GroupCommit()
        self._zkPropertyTransferSvcUrl = None

    def createProperty(self, key, value):
        """
        Returns boolean
        Parameters:
            key: PropertyKeyvalue: T
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # PropertyType
        type = key.getType()
        # String
        path = key.getPath()
        # int
        options = self.constructOptions(type)
        return self._baseDataAccessor.create(path, value.getRecord(), options)


    def setProperty(self, key, value):
        """
        Returns boolean
        Parameters:
            key: PropertyKeyvalue: T
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # PropertyType
        propertyType = key.getType()
        if not value.isValid(): 
            raise HelixException("The ZNRecord for " + propertyType + " is not valid.")


        # String
        path = key.getPath()
        # int
        options = self.constructOptions(propertyType)
#        if propertyType.usePropertyTransferServer():
#            if self._zkPropertyTransferSvcUrl != None and self._zkPropertyTransferClient != None:
#                # ZNRecordUpdate
#                update = ZNRecordUpdate(path, OpCode.SET, value.getRecord())
#                self._zkPropertyTransferClient.enqueueZNRecordUpdate(update, self._zkPropertyTransferSvcUrl)
#                return True


        # boolean
        success = False
        if propertyType in (PropertyType.IDEALSTATES,PropertyType.EXTERNALVIEW):
#            if value.getBucketSize() > 0:
#                # ZNRecord
#                metaRecord = ZNRecord(value.getId())
#                metaRecord.setSimpleFields(value.getRecord().getSimpleFields())
#                success = self._baseDataAccessor.set(path, metaRecord, options)
#                if success:
#                    # ZNRecordBucketizer
#                    bucketizer = ZNRecordBucketizer(value.getBucketSize())
#                    # Map<String, ZNRecord>
#                    map = bucketizer.bucketize(value.getRecord())
#                    # List<String>
#                    paths = []
##                    paths = ArrayList<String>()
#                    # List<ZNRecord>
#                    bucketizedRecords = []
##                    bucketizedRecords = ArrayList<ZNRecord>()
#                    for bucketName in map:
#                        paths.add(path + "/" + bucketName)
#                        bucketizedRecords.add(map.get(bucketName))
#                    self._baseDataAccessor.setChildren(paths, bucketizedRecords, options)
#            else:
#                success = self._baseDataAccessor.set(path, value.getRecord(), options)
            # dzhang: skip the bucketizer
            success = self._baseDataAccessor.set(path, value.getRecord(), options)
        else:
                success = self._baseDataAccessor.set(path, value.getRecord(), options)
        return success


    def updateProperty(self, key, value):
        """
        Returns boolean
        Parameters:
            key: PropertyKeyvalue: T
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # PropertyType
        type = key.getType()
        # String
        path = key.getPath()
        # int
        options = self.constructOptions(type)
        # boolean
        success = False
        if type == PropertyType.CURRENTSTATES:
                success = self._groupCommit.commit(self._baseDataAccessor, options, path, value.getRecord())
        else:
#                if type.usePropertyTransferServer():
#                    if self._zkPropertyTransferSvcUrl != None and self._zkPropertyTransferClient != None:
#                        # ZNRecordUpdate
#                        update = ZNRecordUpdate(path, OpCode.UPDATE, value.getRecord())
#                        self._zkPropertyTransferClient.enqueueZNRecordUpdate(update, self._zkPropertyTransferSvcUrl)
#                        return True
#                    else:
#                        self.LOG.debug("getPropertyTransferUrl is null, skip updating the value")
#                        return True


                success = self._baseDataAccessor.update(path, ZNRecordUpdater(value.getRecord()), options)
        return success


    def getPropertyList(self, keys):
        """
        Returns List<T>
        Parameters:
            keys: List<PropertyKey>
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        if keys == None or keys.size() == 0: 
            return []
#            return Collections.emptyList()

        # List<T>
#        childValues = ArrayList<T>()
        childValues = []
        # List<String>
        paths = []
#        paths = ArrayList<String>()
        for key in keys: paths.append(key.getPath())

        # List<ZNRecord>
        children = self._baseDataAccessor.get(paths, None, 0)
        for i in range(keys.size()): # PropertyKey
            key = keys.get(i)
            # ZNRecord
            record = children.get(i)
            # PropertyType
            type = key.getType()
            # String
            path = key.getPath()
            # int
            options = self.constructOptions(type)
            if type in (PropertyType.CURRENTSTATES, PropertyType.IDEALSTATES, PropertyType.EXTERNALVIEW):
                    if record != None: 
                        # HelixProperty
                        property = HelixProperty(record)
                        # int
                        if bucketSize > 0:
                        # List<ZNRecord>
                            childRecords = self._baseDataAccessor.getChildren(path, None, options)
                            # ZNRecord
                            assembledRecord = ZNRecordAssembler().assemble(childRecords)
                            if assembledRecord != None:
                                record.getSimpleFields().putAll(assembledRecord.getSimpleFields())
                                record.getListFields().putAll(assembledRecord.getListFields())
                                record.getMapFields().putAll(assembledRecord.getMapFields())
                        bucketSize = property.getBucketSize()

            t = HelixProperty.convertToTypedInstance(key.getTypeClass(), record)
#            t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record)
            childValues.add(t)

        return childValues


    def getProperty(self, key):
        """
        Returns T
        Parameters:
            key: PropertyKey
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # PropertyType
        if isinstance(key,list):
            return self.getPropertyList(self,key)
        type = key.getType()
        # String
        path = key.getPath()
        # int
        options = self.constructOptions(type)
        # ZNRecord
        record = None
        try:
            # Stat
            # TODO: come back to zk later
#            stat = Stat()
#            statInit = tuple([None for i in range(len(ZnodeStat._fields))])
            #            stat = ZnodeStat(*statInit)
            stat = HelixZNodeStat()
            record = self._baseDataAccessor.get(path, stat, options)
            if record != None:
                record.setCreationTime(stat.getCtime())
                record.setModifiedTime(stat.getMtime())

#        except zookeeper.ZkNoNodeException, e: pass
        except kazoo.exceptions.NoNodeException, e: pass

        if type in (PropertyType.CURRENTSTATES, PropertyType.IDEALSTATES, PropertyType.EXTERNALVIEW):
                if record != None: 
                    # HelixProperty
                    property = HelixProperty(record)
                    # int
                    bucketSize = property.getBucketSize()
                    if bucketSize > 0: 
                        # List<ZNRecord>
                        childRecords = self._baseDataAccessor.getChildren(path, None, options)
                        # ZNRecord
                        assembledRecord = ZNRecordAssembler().assemble(childRecords)
                        if assembledRecord != None: 
                            record.getSimpleFields().putAll(assembledRecord.getSimpleFields())
                            record.getListFields().putAll(assembledRecord.getListFields())
                            record.getMapFields().putAll(assembledRecord.getMapFields())


        # T
        t = HelixProperty.convertToTypedInstance(key.getTypeClass(), record)
#        t = key.getTypeClass()(record)    # call constructor
        return t


    def removeProperty(self, key):
        """
        Returns boolean
        Parameters:
            key: PropertyKey
        @Override


        """
        # PropertyType
        type = key.getType()
        # String
        path = key.getPath()
        # int
        options = self.constructOptions(type)
        return self._baseDataAccessor.remove(path, options)


    def getChildNames(self, key):
        """
        Returns List<String>
        Parameters:
            key: PropertyKey
        @Override


        """
        # PropertyType
        type = key.getType()
        # String
        parentPath = key.getPath()
        # int
        options = self.constructOptions(type)
        # List<String>
        childNames = self._baseDataAccessor.getChildNames(parentPath, options)
        if childNames == None: 
            childNames = []
#            childNames = Collections.emptyList()

        return childNames


    def getChildValues(self, key):
        """
        Returns List<T>
        Parameters:
            key: PropertyKey
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # PropertyType
        type = key.getType()
        # String
        parentPath = key.getPath()
        # int
        options = self.constructOptions(type)
        # List<T>
        childValues = []
#        childValues = ArrayList<T>()
        # List<ZNRecord>
        children = self._baseDataAccessor.getChildren(parentPath, None, options)
        if children != None: 
            for record in children:
                if type in (PropertyType.CURRENTSTATES, PropertyType.IDEALSTATES, PropertyType.EXTERNALVIEW):
                        if record != None: 
                            # HelixProperty
                            property = HelixProperty(record)
                            # int
                            bucketSize = property.getBucketSize()
                            if bucketSize > 0: 
                                # String
                                childPath = parentPath + "/" + record.getId()
                                # List<ZNRecord>
                                childRecords = self._baseDataAccessor.getChildren(childPath, None, options)
                                # ZNRecord
                                assembledRecord = ZNRecordAssembler().assemble(childRecords)
                                if assembledRecord != None: 
                                    record.getSimpleFields().putAll(assembledRecord.getSimpleFields())
                                    record.getListFields().putAll(assembledRecord.getListFields())
                                    record.getMapFields().putAll(assembledRecord.getMapFields())

                if record != None:
                    # T
                    t = key.getTypeClass()(record)
#                    t = HelixProperty.convertToTypedInstance(key.getTypeClass(), record)
#                    t = (T) HelixProperty.convertToTypedInstance(key.getTypeClass(), record)
                    childValues.append(t)



        return childValues


    def getChildValuesMap(self, key):
        """
        Returns Map<String, T>
        Parameters:
            key: PropertyKey
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # PropertyType
        type = key.getType()
        # String
        parentPath = key.getPath()
        # int
        options = self.constructOptions(type)
        # List<T>
        children = self.getChildValues(key)
        # Map<String, T>
        childValuesMap = {}
#        childValuesMap = HashMap<String, T>()
        for t in children:
          childValuesMap.__setitem__(t.getRecord().getId(), t)

        return childValuesMap


    def keyBuilder(self):
        """
        Returns Builder
        @Override


        """
        return self._propertyKeyBuilder


    def constructOptions(self, type):
        """
        Returns int
        Parameters:
            type: PropertyType
        Java modifiers:
             private

        """
        # int
        options = 0
        if type.isPersistent():
            options = options | AccessOption.PERSISTENT
        else:
            options = options | AccessOption.EPHEMERAL

        return options


    def createChildren(self, keys, children):
        """
        Returns boolean[]
        Parameters:
            keys: List<PropertyKey>children: List<T>
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # int
        options = -1
        # List<String>
        paths = []
#        paths = ArrayList<String>()
        # List<ZNRecord>
        records = []
#        records = ArrayList<ZNRecord>()
        for i in range(len(keys)): # PropertyKey
            key = keys[i]
            # PropertyType
            type = key.getType()
            # String
            path = key.getPath()
            paths.append(path)
            # HelixProperty
            value = children[i]
            records.append(value.getRecord())
            options = self.constructOptions(type)

        return self._baseDataAccessor.createChildren(paths, records, options)


    def setChildren(self, keys, children):
        """
        Returns boolean[]
        Parameters:
            keys: List<PropertyKey>children: List<T>
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # int
        options = -1
        # List<String>
        paths = []
#        paths = ArrayList<String>()
        # List<ZNRecord>
        records = []
#        records = ArrayList<ZNRecord>()
        # List<List<String>>
        bucketizedPaths = [None for key in keys]
#        bucketizedPaths = ArrayList<List<String>>(Collections.nCopies(keys.size(), None))
        # List<List<ZNRecord>>
        bucketizedRecords = [None for key in keys]
#        bucketizedRecords = ArrayList<List<ZNRecord>>(Collections.nCopies(keys.size(), None))
        for i in range(len(keys)): # PropertyKey
            key = keys[i]
            # PropertyType
            type = key.getType()
            # String
            path = key.getPath()
            paths.append(path)
            options = self.constructOptions(type)
            # HelixProperty
            value = children[i]
            if type == PropertyType.EXTERNALVIEW:
#                    if value.getBucketSize() == 0:
                     # do not bucketize
                        records.append(value.getRecord())
#                    else:
#                        self._baseDataAccessor.remove(path, options)
#                        # ZNRecord
#                        metaRecord = ZNRecord(value.getId())
#                        metaRecord.setSimpleFields(value.getRecord().getSimpleFields())
#                        records.append(metaRecord)
#                        # ZNRecordBucketizer
#                        bucketizer = ZNRecordBucketizer(value.getBucketSize())
#                        # Map<String, ZNRecord>
#                        map = bucketizer.bucketize(value.getRecord())
#                        # List<String>
#                        childBucketizedPaths = []
#                        # List<ZNRecord>
#                        childBucketizedRecords = []
#                        for bucketName in map.keys():
#                          childBucketizedPaths.append(path + "/" + bucketName)
#                          childBucketizedRecords.append(map.get(bucketName))
#
#                        bucketizedPaths.__setitem__(i, childBucketizedPaths)
#                        bucketizedRecords.__setitem__(i, childBucketizedRecords)
            else:
                    records.append(value.getRecord())

        # boolean
        success = self._baseDataAccessor.setChildren(paths, records, options)
        # List<String>
        allBucketizedPaths = []
        # List<ZNRecord>
        allBucketizedRecords = []
        for i in range(len(keys)):
            if success[i] and bucketizedPaths.__getitem__(i) != None:
                allBucketizedPaths.extend(bucketizedPaths.__getitem__(i))
                allBucketizedRecords.extend(bucketizedRecords.__getitem__(i))


        self._baseDataAccessor.setChildren(allBucketizedPaths, allBucketizedRecords, options)
        return success


    def getBaseDataAccessor(self):
        """
        Returns BaseDataAccessor<ZNRecord>
        @Override


        """
        return self._baseDataAccessor


    def updateChildren(self, paths, updaters, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>updaters: List<DataUpdater<ZNRecord>>options: int
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        return self._baseDataAccessor.updateChildren(paths, updaters, options)


    def shutdown(self):
        """
        Returns void


        """
        if self._zkPropertyTransferClient != None:
            self._zkPropertyTransferClient.shutdown()



    def onControllerChange(self, changeContext):
        """
        Returns void
        Parameters:
            changeContext: NotificationContext
        @Override


        """
        raise HelixException("Not implemented")
#        self.LOG.info("Controller has changed")
#        self.refreshZkPropertyTransferUrl()
#        if self._zkPropertyTransferClient == None:
#            if self._zkPropertyTransferSvcUrl != None and self._zkPropertyTransferSvcUrl.length() > 0:
#                self.LOG.info("Creating ZkPropertyTransferClient as we get url " + self._zkPropertyTransferSvcUrl)
#                self._zkPropertyTransferClient = ZkPropertyTransferClient(ZkPropertyTransferClient.DEFAULT_MAX_CONCURRENTTASKS)




    def refreshZkPropertyTransferUrl(self):
        """
        Returns void


        """
        try:
            # LiveInstance
            leader = self.getProperty(self.keyBuilder().controllerLeader())
            if leader != None:
                self._zkPropertyTransferSvcUrl = leader.getWebserviceUrl()
                self.LOG.info("_zkPropertyTransferSvcUrl : " + self._zkPropertyTransferSvcUrl + " Controller " + leader.getInstanceName())
            else:
                self._zkPropertyTransferSvcUrl = None

        except Exception, e:
            self._zkPropertyTransferSvcUrl = None




