# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import File
#from java.util import List
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger
#from org.apache.zookeeper.data import Stat
from org.apache.helix.AccessOption import AccessOption
from org.apache.helix.BaseDataAccessor import BaseDataAccessor
from org.apache.helix.store.zk.ZNode import ZNode


# Parameterized type: <T>
class WriteThroughCache(Cache<T>):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(WriteThroughCache.class)



    """

    Parameters:
        BaseDataAccessor<T> accessor
        List<String> paths
    """
    def __init__(self, accessor, paths):
        super()
        self._accessor = accessor
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
        else:
            znode.setData(data)
            znode.setStat(stat)



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
            # Stat
            stat = Stat()
            # T
            readData = _accessor.get(path, stat, AccessOption.THROW_EXCEPTION_IFNOTEXIST)
            update(path, readData, stat)
            # ZNode
            znode = _cache.get(path)
            # List<String>
            childNames = _accessor.getChildNames(path, 0)
            if childNames != None && childNames.size() > 0: 
                for # String
                childName = None
                 in childNames) # String
                    childPath = path + "/" + childName
                    if not znode.hasChild(childName): 
                        znode.addChild(childName)
                        updateRecursive(childPath)



        except ZkNoNodeException, e:
        final:
                _lock.writeLock().unlock()




