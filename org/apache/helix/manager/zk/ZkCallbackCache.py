# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import File
#from java.util import List
#from java.util import Map
#from java.util import Set
#from java.util.concurrent import ConcurrentHashMap
#from java.util.concurrent import CopyOnWriteArraySet
#from org.I0Itec.zkclient import IZkChildListener
#from org.I0Itec.zkclient import IZkDataListener
#from org.I0Itec.zkclient import IZkStateListener
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger
#from org.apache.zookeeper.Watcher.Event import EventType
#from org.apache.zookeeper.Watcher.Event import KeeperState
#from org.apache.zookeeper.data import Stat
from org.apache.helix.AccessOption import AccessOption
from org.apache.helix.BaseDataAccessor import BaseDataAccessor
from org.apache.helix.manager.zk.ZkCacheEventThread import ZkCacheEvent
from org.apache.helix.store.HelixPropertyListener import HelixPropertyListener
from org.apache.helix.store.zk.ZNode import ZNode


# Parameterized type: <T>
class ZkCallbackCache(Cache<T>, IZkChildListener, IZkDataListener, IZkStateListener):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(ZkCallbackCache.class)









    """

    Parameters:
        BaseDataAccessor<T> accessor
        String chrootPath
        List<String> paths
        ZkCacheEventThread eventThread
    """
    def __init__(self, accessor, chrootPath, paths, eventThread):
        super()
        self._accessor = accessor
        self._chrootPath = chrootPath
        self._listener = ConcurrentHashMap<String, Set<HelixPropertyListener>>()
        self._eventThread = eventThread
        if paths != None && not paths.isEmpty(): 
            for # String
            path = None
             in paths) updateRecursive(path)




    def update(self, path, data, stat):
        """
        Returns void
        Parameters:
            path: Stringdata: Tstat: Stat
        @Override


        """
        # String
        parentPath = File(path).getParent()
        # String
        childName = File(path).getName()
        addToParentChildSet(parentPath, childName)
        # ZNode
        znode = _cache.get(path)
        if znode == None: 
            _cache.put(path, ZNode(path, data, stat))
            fireEvents(path, EventType.NodeCreated)
        else:
            # Stat
            oldStat = znode.getStat()
            znode.setData(data)
            znode.setStat(stat)
            if oldStat.getCzxid() != stat.getCzxid(): 
                fireEvents(path, EventType.NodeDeleted)
                fireEvents(path, EventType.NodeCreated)
            else:
                if oldStat.getVersion() != stat.getVersion(): 
                    fireEvents(path, EventType.NodeDataChanged)




    def updateRecursive(self, path):
        """
        Returns void
        Parameters:
            path: String
        @Override


        """
        if path == None: 
            return

        try:
            _lock.writeLock().lock()
            try:
                _accessor.subscribeDataChanges(path, self)
                # Stat
                stat = Stat()
                # T
                readData = _accessor.get(path, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST)
                update(path, readData, stat)
            except ZkNoNodeException, e:

            # ZNode
            znode = _cache.get(path)
            # List<String>
            childNames = _accessor.subscribeChildChanges(path, self)
            if childNames != None && not childNames.isEmpty(): 
                for # String
                childName = None
                 in childNames) if not znode.hasChild(childName): 
                        # String
                        childPath = path + "/" + childName
                        znode.addChild(childName)
                        updateRecursive(childPath)



        final:
                _lock.writeLock().unlock()



    def handleChildChange(self, parentPath, currentChilds):
        """
        Returns void
        Parameters:
            parentPath: StringcurrentChilds: List<String>
        @Override


        Throws: 
            Exception
        """
        if currentChilds == None: 
            return

        updateRecursive(parentPath)


    def handleDataChange(self, dataPath, data):
        """
        Returns void
        Parameters:
            dataPath: Stringdata: Object
        @Override


        Throws: 
            Exception
        """
        try:
            _lock.writeLock().lock()
            # Stat
            stat = Stat()
            # Object
            readData = _accessor.get(dataPath, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST)
            # ZNode
            znode = _cache.get(dataPath)
            if znode != None: 
                # Stat
                oldStat = znode.getStat()
                znode.setData(readData)
                znode.setStat(stat)
                if oldStat.getCzxid() != stat.getCzxid(): 
                    fireEvents(dataPath, EventType.NodeDeleted)
                    fireEvents(dataPath, EventType.NodeCreated)
                else:
                    if oldStat.getVersion() != stat.getVersion(): 
                        fireEvents(dataPath, EventType.NodeDataChanged)

            else:

        final:
                _lock.writeLock().unlock()



    def handleDataDeleted(self, dataPath):
        """
        Returns void
        Parameters:
            dataPath: String
        @Override


        Throws: 
            Exception
        """
        try:
            _lock.writeLock().lock()
            _accessor.unsubscribeDataChanges(dataPath, self)
            _accessor.unsubscribeChildChanges(dataPath, self)
            # String
            parentPath = File(dataPath).getParent()
            # String
            name = File(dataPath).getName()
            removeFromParentChildSet(parentPath, name)
            _cache.remove(dataPath)
            fireEvents(dataPath, EventType.NodeDeleted)
        final:
                _lock.writeLock().unlock()



    def handleStateChanged(self, state):
        """
        Returns void
        Parameters:
            state: KeeperState
        @Override


        Throws: 
            Exception
        """


    def handleNewSession(self):
        """
        Returns void
        @Override


        Throws: 
            Exception
        """


    def subscribe(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: HelixPropertyListener


        """
        synchronized (_listener) # Set<HelixPropertyListener>
            listeners = _listener.get(path)
            if listeners == None: 
                listeners = CopyOnWriteArraySet<HelixPropertyListener>()
                _listener.put(path, listeners)

            listeners.add(listener)



    def unsubscribe(self, path, childListener):
        """
        Returns void
        Parameters:
            path: StringchildListener: HelixPropertyListener


        """
        synchronized (_listener) # Set<HelixPropertyListener>
            listeners = _listener.get(path)
            if listeners != None: 
                listeners.remove(childListener)




    def fireEvents(self, path, type):
        """
        Returns void
        Parameters:
            path: Stringtype: EventType
        Java modifiers:
             private

        """
        # String
        tmpPath = path
        # String
        clientPath = (java2python_runtime.ternary(_chrootPath == None, path, (java2python_runtime.ternary((_chrootPath == path), "/", path.substring(_chrootPath.length())))))
        while (tmpPath != None:
            # Set<HelixPropertyListener>
            listeners = _listener.get(tmpPath)
            if listeners != None && not listeners.isEmpty(): 
                for # HelixPropertyListener
                listener = None
                 in listeners) try:
                        switch(type) {
                            case NodeDataChanged:
                                _eventThread.send(ZkCacheEvent("dataChange on " + path + " send to " + listener) {

                                    def run(self):
                                        """
                                        Returns void
                                        @Override


                                        Throws: 
                                            Exception
                                        """
                                        listener.onDataChange(clientPath)

                                })
                                break
                            case NodeCreated:
                                _eventThread.send(ZkCacheEvent("dataCreate on " + path + " send to " + listener) {

                                    def run(self):
                                        """
                                        Returns void
                                        @Override


                                        Throws: 
                                            Exception
                                        """
                                        listener.onDataCreate(clientPath)

                                })
                                break
                            case NodeDeleted:
                                _eventThread.send(ZkCacheEvent("dataDelete on " + path + " send to " + listener) {

                                    def run(self):
                                        """
                                        Returns void
                                        @Override


                                        Throws: 
                                            Exception
                                        """
                                        listener.onDataDelete(clientPath)

                                })
                                break
                            default:
                                break
                        }
                    except Exception, e:
                        LOG.error("Exception in handle events."+ str(e))



            tmpPath = File(tmpPath).getParent()




