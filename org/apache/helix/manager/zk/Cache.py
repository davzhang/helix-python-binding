# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import File
#from java.util import List
#from java.util import Map
#from java.util import Set
#from java.util.concurrent import ConcurrentHashMap
#from java.util.concurrent.locks import ReadWriteLock
#from java.util.concurrent.locks import ReentrantReadWriteLock
#from org.apache.zookeeper.data import Stat
from org.apache.helix.store.zk.ZNode import ZNode

Java modifiers:
     abstract
# Parameterized type: <T>
class Cache:





    """

    """
    def __init__(self):
        self._lock = ReentrantReadWriteLock()
        self._cache = ConcurrentHashMap<String, ZNode>()


    def addToParentChildSet(self, parentPath, childName):
        """
        Returns void
        Parameters:
            parentPath: StringchildName: String


        """
        # ZNode
        znode = _cache.get(parentPath)
        if znode != None: 
            znode.addChild(childName)



    def addToParentChildSet(self, parentPath, childNames):
        """
        Returns void
        Parameters:
            parentPath: StringchildNames: List<String>


        """
        if childNames != None && not childNames.isEmpty(): 
            # ZNode
            znode = _cache.get(parentPath)
            if znode != None: 
                znode.addChildren(childNames)




    def removeFromParentChildSet(self, parentPath, name):
        """
        Returns void
        Parameters:
            parentPath: Stringname: String


        """
        # ZNode
        zNode = _cache.get(parentPath)
        if zNode != None: 
            zNode.removeChild(name)



    def exists(self, path):
        """
        Returns boolean
        Parameters:
            path: String


        """
        return _cache.containsKey(path)


    def get(self, path):
        """
        Returns ZNode
        Parameters:
            path: String


        """
        try:
            _lock.readLock().lock()
            return _cache.get(path)
        final:
                _lock.readLock().unlock()



    def lockWrite(self):
        """
        Returns void


        """
        _lock.writeLock().lock()


    def unlockWrite(self):
        """
        Returns void


        """
        _lock.writeLock().unlock()


    def lockRead(self):
        """
        Returns void


        """
        _lock.readLock().lock()


    def unlockRead(self):
        """
        Returns void


        """
        _lock.readLock().unlock()


    def purgeRecursive(self, path):
        """
        Returns void
        Parameters:
            path: String


        """
        try:
            _lock.writeLock().lock()
            # String
            parentPath = File(path).getParent()
            # String
            name = File(path).getName()
            removeFromParentChildSet(parentPath, name)
            # ZNode
            znode = _cache.remove(path)
            if znode != None: 
                # Set<String>
                childNames = znode.getChildSet()
                for # String
                childName = None
                 in childNames) # String
                    childPath = path + "/" + childName
                    purgeRecursive(childPath)


        final:
                _lock.writeLock().unlock()



    def reset(self):
        """
        Returns void


        """
        try:
            _lock.writeLock().lock()
            _cache.clear()
        final:
                _lock.writeLock().unlock()



    def update(self, path, data, stat):
        """
        Returns void
        Parameters:
            path: Stringdata: Tstat: Stat
        Java modifiers:
             abstract

        """
        pass


    def updateRecursive(self, path):
        """
        Returns void
        Parameters:
            path: String
        Java modifiers:
             abstract

        """
        pass


    def getCache(self):
        """
        Returns Map<String, ZNode>


        """
        return _cache



