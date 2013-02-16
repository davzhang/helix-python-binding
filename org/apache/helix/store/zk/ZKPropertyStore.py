# package org.apache.helix.store.zk
#from org.apache.helix.store.zk import *
#from java.util import ArrayList
#from java.util import Collections
#from java.util import Comparator
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from java.util.concurrent import ConcurrentHashMap
#from org.I0Itec.zkclient import DataUpdater
#from org.I0Itec.zkclient import IZkDataListener
#from org.I0Itec.zkclient import IZkStateListener
#from org.I0Itec.zkclient import ZkConnection
#from org.I0Itec.zkclient.exception import ZkBadVersionException
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger
#from org.apache.zookeeper.Watcher.Event import KeeperState
#from org.apache.zookeeper import ZooKeeper
#from org.apache.zookeeper.data import Stat
from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.store.PropertyChangeListener import PropertyChangeListener
from org.apache.helix.store.PropertySerializer import PropertySerializer
from org.apache.helix.store.PropertyStat import PropertyStat
from org.apache.helix.store.PropertyStore import PropertyStore
from org.apache.helix.store.PropertyStoreException import PropertyStoreException

@Deprecated

# Parameterized type: <T>
class ZKPropertyStore(PropertyStore<T>, IZkStateListener, IZkDataListener):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(ZKPropertyStore.class)


    class ByteArrayUpdater(DataUpdater<byte[]>):





        """

        Parameters:
            DataUpdater<T> updater
            PropertySerializer<T> serializer
        """
        def __init__(self, updater, serializer):
            self._updater = updater
            self._serializer = serializer


        def update(self, current):
            """
            Returns byte[]
            Parameters:
                current: byte[]
            @Override


            """
            try:
                # T
                currentValue = None
                if current != None: 
                    currentValue = _serializer.deserialize(current)

                # T
                updateValue = _updater.update(currentValue)
                return _serializer.serialize(updateValue)
            except PropertyStoreException, e:
                LOG.error("Exception in update. Updater: " + str(_updater)+ str(e))

            return None

















    """

    Parameters:
        ZkClient zkClient
        PropertySerializer<T> serializer
        String root
    """
    def __init__(self, zkClient, serializer, root):
        if zkClient == None or serializer == None or root == None: 
            raise IllegalArgumentException("zkClient|serializer|root can't be null")


        self._root = normalizeKey(root)
        self._zkClient = zkClient
        setPropertySerializer(serializer)
        self._zkClient.createPersistent(_root, True)
        self._zkClient.subscribeStateChanges(self)


    def normalizeKey(self, key):
        """
        Returns String
        Parameters:
            key: String
        Java modifiers:
             private

        """
        if key == None: 
            LOG.error("Key can't be null")
            raise IllegalArgumentException("Key can't be null")


        while (key.startsWith("/"):
            key = key.substring(1)

        return "/" + key


    def getAbsolutePath(self, key):
        """
        Returns String
        Parameters:
            key: String
        Java modifiers:
             private

        """
        key = normalizeKey(key)
        if (key == "/"): 
            return _root
        else:
            return _root + key



    def getRelativePath(self, path):
        """
        Returns String
        Parameters:
            path: String


        """
        if not path.startsWith(_root): 
            # String
            errMsg = path + "does NOT start with property store's root: " + _root
            LOG.error(errMsg)
            raise IllegalArgumentException(errMsg)


        if (path == _root): 
            return "/"
        else:
            return path.substring(_root.length())



    def createPropertyNamespace(self, prefix):
        """
        Returns void
        Parameters:
            prefix: String
        @Override


        Throws: 
            PropertyStoreException
        """
        # String
        path = getAbsolutePath(prefix)
        try:
            if not _zkClient.exists(path): 
                _zkClient.createPersistent(path, True)

        except Exception, e:
            LOG.error("Exception in creatPropertyNamespace(" + str(prefix)+ ")"+ str(e))
            raise PropertyStoreException(e.toString())




    def setProperty(self, key, value):
        """
        Returns void
        Parameters:
            key: Stringvalue: T
        @Override


        Throws: 
            PropertyStoreException
        """
        # String
        path = getAbsolutePath(key)
        try:
            if not _zkClient.exists(path): 
                _zkClient.createPersistent(path, True)

            # byte[]
            valueBytes = _serializer.serialize(value)
            _zkClient.writeData(path, valueBytes)
        except Exception, e:
            LOG.error("Exception when setProperty(" + str(key)+ ", " + str(value)+ ")"+ str(e))
            raise PropertyStoreException(e.toString())




    def getProperty(self, key):
        """
        Returns T
        Parameters:
            key: String
        @Override


        Throws: 
            PropertyStoreException
        """
        return getProperty(key, None)


    def getValueAndStat(self, bytes, stat, propertyStat):
        """
        Returns T
        Parameters:
            bytes: byte[]stat: StatpropertyStat: PropertyStat
        Java modifiers:
             private

        Throws: 
            PropertyStoreException
        """
        # T
        value = _serializer.deserialize(bytes)
        if propertyStat != None: 
            propertyStat.setLastModifiedTime(stat.getMtime())
            propertyStat.setVersion(stat.getVersion())

        return value


    def getProperty(self, key, propertyStat):
        """
        Returns T
        Parameters:
            key: StringpropertyStat: PropertyStat
        @Override


        Throws: 
            PropertyStoreException
        """
        # String
        normalizedKey = normalizeKey(key)
        # String
        path = getAbsolutePath(normalizedKey)
        # Stat
        stat = Stat()
        # T
        value = None
        try:
            synchronized (_cache) # PropertyItem
                item = _cache.get(normalizedKey)
                _zkClient.subscribeDataChanges(path, self)
                if item != None: 
                    stat = _zkClient.getStat(path)
                    if stat != None: 
                        if item._stat.getCzxid() != stat.getCzxid() or item.getVersion() < stat.getVersion(): 
                            # byte[]
                            bytes = _zkClient.readDataAndStat(path, stat, True)
                            if bytes != None: 
                                value = getValueAndStat(bytes, stat, propertyStat)
                                _cache.put(normalizedKey, PropertyItem(bytes, stat))

                        else:
                            value = getValueAndStat(item.getBytes(), stat, propertyStat)


                else:
                    # byte[]
                    bytes = _zkClient.readDataAndStat(path, stat, True)
                    if bytes != None: 
                        value = getValueAndStat(bytes, stat, propertyStat)
                        _cache.put(normalizedKey, PropertyItem(bytes, stat))



            return value
        except Exception, e:
            LOG.error("Exception in getProperty(" + str(key)+ ")"+ str(e))
            raise (PropertyStoreException(e.toString()))




    def removeProperty(self, key):
        """
        Returns void
        Parameters:
            key: String
        @Override


        Throws: 
            PropertyStoreException
        """
        # String
        normalizedKey = normalizeKey(key)
        # String
        path = getAbsolutePath(normalizedKey)
        try:
            _zkClient.delete(path)
        except ZkNoNodeException, e:
        except Exception, e:
            LOG.error("Exception in removeProperty(" + str(key)+ ")"+ str(e))
            raise (PropertyStoreException(e.toString()))




    def getPropertyRootNamespace(self):
        """
        Returns String
        @Override


        """
        return _root


    def removeNamespace(self, prefix):
        """
        Returns void
        Parameters:
            prefix: String
        @Override


        Throws: 
            PropertyStoreException
        """
        # String
        path = getAbsolutePath(prefix)
        try:
            _zkClient.deleteRecursive(path)
        except ZkNoNodeException, e:
        except Exception, e:
            LOG.error("Exception in removeProperty(" + str(prefix)+ ")"+ str(e))
            raise (PropertyStoreException(e.toString()))




    def doGetPropertyNames(self, prefix, leafNodes):
        """
        Returns void
        Parameters:
            prefix: StringleafNodes: List<String>
        Java modifiers:
             private

        Throws: 
            PropertyStoreException
        """
        # String
        path = getAbsolutePath(prefix)
        if not _zkClient.exists(path): 
            return

        # List<String>
        childs = _zkClient.getChildren(path)
        if childs == None: 
            return

        if childs.size() == 0: 
            leafNodes.add(prefix)
            return

        for # String
        child = None
         in childs) # String
            childPath = java2python_runtime.ternary((prefix == "/"), prefix + child, prefix + "/" + child)
            doGetPropertyNames(childPath, leafNodes)



    def getPropertyNames(self, prefix):
        """
        Returns List<String>
        Parameters:
            prefix: String
        @Override


        Throws: 
            PropertyStoreException
        """
        # String
        normalizedKey = normalizeKey(prefix)
        # List<String>
        propertyNames = ArrayList<String>()
        doGetPropertyNames(normalizedKey, propertyNames)
        if propertyNames.size() > 1: 
            Collections.sort(propertyNames)

        return propertyNames


    def setPropertyDelimiter(self, delimiter):
        """
        Returns void
        Parameters:
            delimiter: String
        @Override


        Throws: 
            PropertyStoreException
        """
        raise PropertyStoreException("setPropertyDelimiter() not implemented for ZKPropertyStore")



    def subscribeForPropertyChange(self, prefix, listener):
        """
        Returns void
        Parameters:
            prefix: Stringlistener: PropertyChangeListener<T>
        @Override


        Throws: 
            PropertyStoreException
        """
        if listener == None: 
            raise IllegalArgumentException("listener can't be null. Prefix: " + prefix)


        # String
        path = getAbsolutePath(prefix)
        # ZkCallbackHandler<T>
        callback = None
        synchronized (_callbackMap) # Map<PropertyChangeListener<T>, ZkCallbackHandler<T>>
            callbacks = None
            
            if not _callbackMap.containsKey(path): 
                _callbackMap.put(path, HashMap<PropertyChangeListener<T>, ZkCallbackHandler<T>>())

            callbacks = _callbackMap.get(path)
            if not callbacks.containsKey(listener): 
                callback = ZkCallbackHandler<T>(_zkClient, self, prefix, listener)
                callbacks.put(listener, callback)


        try:
            if callback != None: 
                _zkClient.subscribeDataChanges(path, callback)
                _zkClient.subscribeChildChanges(path, callback)
                callback.handleChildChange(path, _zkClient.getChildren(path))
                LOG.debug("Subscribed changes for " + str(path))

        except Exception, e:
            LOG.error("Exception in subscribeForPropertyChange(" + str(prefix)+ ")"+ str(e))
            raise (PropertyStoreException(e.toString()))




    def doUnsubscribeForPropertyChange(self, prefix, callback):
        """
        Returns void
        Parameters:
            prefix: Stringcallback: ZkCallbackHandler<T>
        Java modifiers:
             private

        """
        # String
        path = getAbsolutePath(prefix)
        _zkClient.unsubscribeDataChanges(path, callback)
        _zkClient.unsubscribeChildChanges(path, callback)
        # List<String>
        childs = _zkClient.getChildren(path)
        if childs == None or childs.size() == 0: 
            return

        for # String
        child = None
         in childs) doUnsubscribeForPropertyChange(prefix + "/" + child, callback)



    def unsubscribeForPropertyChange(self, prefix, listener):
        """
        Returns void
        Parameters:
            prefix: Stringlistener: PropertyChangeListener<T>
        @Override


        Throws: 
            PropertyStoreException
        """
        if listener == None: 
            raise IllegalArgumentException("listener can't be null. Prefix: " + prefix)


        # String
        path = getAbsolutePath(prefix)
        # ZkCallbackHandler<T>
        callback = None
        synchronized (_callbackMap) if _callbackMap.containsKey(path): 
                # Map<PropertyChangeListener<T>, ZkCallbackHandler<T>>
                callbacks = _callbackMap.get(path)
                callback = callbacks.remove(listener)
                if callbacks == None or callbacks.isEmpty(): 
                    _callbackMap.remove(path)



        if callback != None: 
            doUnsubscribeForPropertyChange(prefix, callback)
            LOG.debug("Unsubscribed changes for " + str(path))



    def canParentStoreData(self):
        """
        Returns boolean
        @Override


        """
        return False


    def setPropertySerializer(self, serializer):
        """
        Returns void
        Parameters:
            serializer: PropertySerializer<T>
        @Override


        """
        if serializer == None: 
            raise IllegalArgumentException("serializer can't be null")


        _serializer = serializer


    def updatePropertyUntilSucceed(self, key, updater):
        """
        Returns void
        Parameters:
            key: Stringupdater: DataUpdater<T>
        @Override


        Throws: 
            PropertyStoreException
        """
        updatePropertyUntilSucceed(key, updater, True)


    def updatePropertyUntilSucceed(self, key, updater, createIfAbsent):
        """
        Returns void
        Parameters:
            key: Stringupdater: DataUpdater<T>createIfAbsent: boolean
        @Override


        Throws: 
            PropertyStoreException
        """
        # String
        path = getAbsolutePath(key)
        try:
            if not _zkClient.exists(path): 
                if not createIfAbsent: 
                    raise PropertyStoreException("Can't update " + key + " since no node exists")

                else:
                    _zkClient.createPersistent(path, True)


            _zkClient.updateDataSerialized(path, ByteArrayUpdater(updater, _serializer))
        except Exception, e:
            LOG.error("Exception in updatePropertyUntilSucceed(" + str(key)+ ", " + str(createIfAbsent)+ ")"+ str(e))
            raise (PropertyStoreException(e.toString()))




    def compareAndSet(self, key, expected, update, comparator):
        """
        Returns boolean
        Parameters:
            key: Stringexpected: Tupdate: Tcomparator: Comparator<T>
        @Override


        """
        return compareAndSet(key, expected, update, comparator, True)


    def compareAndSet(self, key, expected, update, comparator, createIfAbsent):
        """
        Returns boolean
        Parameters:
            key: Stringexpected: Tupdate: Tcomparator: Comparator<T>createIfAbsent: boolean
        @Override


        """
        # String
        path = getAbsolutePath(key)
        if not _zkClient.exists(path): 
            if createIfAbsent: 
                _zkClient.createPersistent(path, True)
            else:
                return False


        try:
            # Stat
            stat = Stat()
            # byte[]
            currentBytes = _zkClient.readDataAndStat(path, stat, True)
            # T
            current = None
            if currentBytes != None: 
                current = _serializer.deserialize(currentBytes)

            if comparator.compare(current, expected) == 0: 
                # byte[]
                valueBytes = _serializer.serialize(update)
                _zkClient.writeData(path, valueBytes, stat.getVersion())
                return True

        except ZkBadVersionException, e:
            LOG.warn("Get BadVersion when writing to zookeeper. Mostly Ignorable due to contention")
        except Exception, e:
            LOG.error("Exception when compareAndSet(" + str(key)+ ")"+ str(e))

        return False


    def exists(self, key):
        """
        Returns boolean
        Parameters:
            key: String
        @Override


        """
        # String
        path = getAbsolutePath(key)
        return _zkClient.exists(path)


    def handleStateChanged(self, state):
        """
        Returns void
        Parameters:
            state: KeeperState
        @Override


        Throws: 
            Exception
        """
        LOG.info("KeeperState:" + state)
        switch(state) {
            case SyncConnected:
                _isConnected = True
                break
            case Disconnected:
                _isConnected = False
                break
            case Expired:
                _isConnected = False
                _hasSessionExpired = True
                break
        }


    def handleNewSession(self):
        """
        Returns void
        @Override


        Throws: 
            Exception
        """
        # ZkConnection
        connection = ((ZkConnection) _zkClient.getConnection())
        # ZooKeeper
        zookeeper = connection.getZookeeper()
        LOG.info("handleNewSession: " + zookeeper.getSessionId())
        synchronized (_callbackMap) for # String
            path = None
             in _callbackMap.keySet()) # Map<PropertyChangeListener<T>, ZkCallbackHandler<T>>
                callbacks = _callbackMap.get(path)
                if callbacks == None or callbacks.size() == 0: 
                    LOG.error("Get a null callback map. Remove it. Path: " + str(path))
                    _callbackMap.remove(path)
                    continue

                for # PropertyChangeListener<T>
                listener = None
                 in callbacks.keySet()) # ZkCallbackHandler<T>
                    callback = callbacks.get(listener)
                    if callback == None: 
                        LOG.error("Get a null callback. Remove it. Path: " + str(path)+ ", listener: " + str(listener))
                        callbacks.remove(listener)
                        continue

                    _zkClient.subscribeDataChanges(path, callback)
                    _zkClient.subscribeChildChanges(path, callback)
                    callback.handleChildChange(path, _zkClient.getChildren(path))





    def start(self):
        """
        Returns boolean
        @Override


        """
        return False


    def stop(self):
        """
        Returns boolean
        @Override


        """
        _zkClient.close()
        return True


    def handleDataChange(self, dataPath, data):
        """
        Returns void
        Parameters:
            dataPath: Stringdata: Object
        @Override


        Throws: 
            Exception
        """


    def handleDataDeleted(self, dataPath):
        """
        Returns void
        Parameters:
            dataPath: String
        @Override


        Throws: 
            Exception
        """
        # String
        key = getRelativePath(dataPath)
        synchronized (_cache) _zkClient.unsubscribeDataChanges(dataPath, self)
            _cache.remove(key)




