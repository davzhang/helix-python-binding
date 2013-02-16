# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import File
#from java.util import ArrayList
#from java.util import Arrays
#from java.util import Collections
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from java.util.concurrent import ConcurrentHashMap
#from org.apache.log4j import Logger
#from org.apache.zookeeper.data import Stat
from org.apache.helix.DataAccessor import DataAccessor
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.PropertyPathConfig import PropertyPathConfig
from org.apache.helix.PropertyType import PropertyType
from org.apache.helix.ZNRecord import ZNRecord

@Deprecated

class ZKDataAccessor(DataAccessor):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = Logger.getLogger(ZKDataAccessor.class)







    """

    Parameters:
        String clusterName
        ZkClient zkClient
    """
    def __init__(self, clusterName, zkClient):
        self._clusterName = clusterName
        self._zkClient = zkClient


    def setProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: HelixPropertykeys: String
        @Override


        """
        if not value.isValid(): 
            raise HelixException("The ZNRecord for " + type + " is not valid.")


        return setProperty(type, value.getRecord(), keys)


    def setProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: ZNRecordkeys: String
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(type, _clusterName, keys)
        # String
        parent = File(path).getParent()
        if not _zkClient.exists(parent): 
            _zkClient.createPersistent(parent, True)

        if _zkClient.exists(path): 
            if type.isCreateOnlyIfAbsent(): 
                return False
            else:
                ZKUtil.createOrUpdate(_zkClient, path, value, type.isPersistent(), False)

        else:
            try:
                if type.isPersistent(): 
                    _zkClient.createPersistent(path, value)
                else:
                    _zkClient.createEphemeral(path, value)

            except Exception, e:
                logger.warn("Exception while creating path:" + path + " Most likely due to race condition(Ignorable).", e)
                return False


        return True


    def updateProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: HelixPropertykeys: String
        @Override


        """
        return updateProperty(type, value.getRecord(), keys)


    def updateProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: ZNRecordkeys: String
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(type, _clusterName, keys)
        if type.isUpdateOnlyOnExists(): 
            ZKUtil.updateIfExists(_zkClient, path, value, type.isMergeOnUpdate())
        else:
            # String
            parent = File(path).getParent()
            if not _zkClient.exists(parent): 
                _zkClient.createPersistent(parent, True)

            if not type.usePropertyTransferServer(): 
                ZKUtil.createOrUpdate(_zkClient, path, value, type.isPersistent(), type.isMergeOnUpdate())
            else:
                ZKUtil.asyncCreateOrUpdate(_zkClient, path, value, type.isPersistent(), type.isMergeOnUpdate())


        return True


    def getProperty(self, clazz, type, keys):
        """
        Returns T
        Parameters:
            clazz: Class<T>type: PropertyTypekeys: String
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        return HelixProperty.convertToTypedInstance(clazz, getProperty(type, keys))


    def getProperty(self, type, keys):
        """
        Returns ZNRecord
        Parameters:
            type: PropertyTypekeys: String
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(type, _clusterName, keys)
        if not type.isCached(): 
            return _zkClient.readData(path, True)
        else:
            # int
            len = keys.length
            if len == 0: 
                return _zkClient.readData(path, True)
            else:
                # String[]
                subkeys = Arrays.copyOfRange(keys, 0, len - 1)
                # Map<String, ZNRecord>
                newChilds = refreshChildValuesCache(type, subkeys)
                return newChilds.get(keys[len - 1])




    def removeProperty(self, type, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypekeys: String
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(type, _clusterName, keys)
        return _zkClient.delete(path)


    def getChildNames(self, type, keys):
        """
        Returns List<String>
        Parameters:
            type: PropertyTypekeys: String
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(type, _clusterName, keys)
        if _zkClient.exists(path): 
            return _zkClient.getChildren(path)
        else:
            return Collections.emptyList()



    def getChildValues(self, clazz, type, keys):
        """
        Returns List<T>
        Parameters:
            clazz: Class<T>type: PropertyTypekeys: String
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # List<ZNRecord>
        newChilds = getChildValues(type, keys)
        if newChilds.size() > 0: 
            return HelixProperty.convertToTypedList(clazz, newChilds)

        return Collections.emptyList()


    def getChildValues(self, type, keys):
        """
        Returns List<ZNRecord>
        Parameters:
            type: PropertyTypekeys: String
        @Override


        """
        # String
        path = PropertyPathConfig.getPath(type, _clusterName, keys)
        if _zkClient.exists(path): 
            if not type.isCached(): 
                return ZKUtil.getChildren(_zkClient, path)
            else:
                # Map<String, ZNRecord>
                newChilds = refreshChildValuesCache(type, keys)
                return ArrayList<ZNRecord>(newChilds.values())


        return Collections.emptyList()


    def reset(self):
        """
        Returns void


        """
        _cache.clear()


    def refreshChildValuesCache(self, type, keys):
        """
        Returns Map<String, ZNRecord>
        Parameters:
            type: PropertyTypekeys: String
        Java modifiers:
             private

        """
        if not type.isCached(): 
            raise IllegalArgumentException("Type:" + type + " is NOT cached")


        # String
        path = PropertyPathConfig.getPath(type, _clusterName, keys)
        # Map<String, ZNRecord>
        newChilds = refreshChildValues(path, _cache.get(path))
        if newChilds != None && newChilds.size() > 0: 
            _cache.put(path, newChilds)
            return newChilds
        else:
            _cache.remove(path)
            return Collections.emptyMap()



    def refreshChildValues(self, parentPath, oldChildRecords):
        """
        Returns Map<String, ZNRecord>
        Parameters:
            parentPath: StringoldChildRecords: Map<String, ZNRecord>
        Java modifiers:
             private

        """
        # List<String>
        childs = _zkClient.getChildren(parentPath)
        if childs == None or childs.size() == 0: 
            return Collections.emptyMap()

        # Stat
        newStat = Stat()
        # Map<String, ZNRecord>
        newChildRecords = HashMap<String, ZNRecord>()
        for # String
        child = None
         in childs) # String
            childPath = parentPath + "/" + child
            if oldChildRecords == None or not oldChildRecords.containsKey(child): 
                # ZNRecord
                record = _zkClient.readDataAndStat(childPath, newStat, True)
                if record != None: 
                    record.setVersion(newStat.getVersion())
                    newChildRecords.put(child, record)

            else:
                # ZNRecord
                oldChild = oldChildRecords.get(child)
                # int
                oldVersion = oldChild.getVersion()
                # long
                oldCtime = oldChild.getCreationTime()
                newStat = _zkClient.getStat(childPath)
                if newStat != None: 
                    if oldCtime < newStat.getCtime() or oldVersion < newStat.getVersion(): 
                        # ZNRecord
                        record = _zkClient.readDataAndStat(childPath, newStat, True)
                        if record != None: 
                            record.setVersion(newStat.getVersion())
                            record.setCreationTime(newStat.getCtime())
                            record.setModifiedTime(newStat.getMtime())
                            newChildRecords.put(child, record)

                    else:
                        newChildRecords.put(child, oldChild)




        return Collections.unmodifiableMap(newChildRecords)


    def getChildValuesMap(self, clazz, type, keys):
        """
        Returns Map<String, T>
        Parameters:
            clazz: Class<T>type: PropertyTypekeys: String
        @Override


        Parameterized: <T extends HelixProperty> 
        """
        # List<T>
        list = getChildValues(clazz, type, keys)
        return Collections.unmodifiableMap(HelixProperty.convertListToMap(list))



