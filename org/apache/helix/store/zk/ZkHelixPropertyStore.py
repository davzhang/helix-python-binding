# package org.apache.helix.store.zk
#from org.apache.helix.store.zk import *
#from java.util import List
#from org.I0Itec.zkclient.serialize import ZkSerializer
from org.apache.helix.manager.zk.ZkBaseDataAccessor import ZkBaseDataAccessor
#from org.apache.helix.manager.zk.ZkCacheBaseDataAccessor import ZkCacheBaseDataAccessor


# Parameterized type: <T>
#class ZkHelixPropertyStore(ZkCacheBaseDataAccessor):
class ZkHelixPropertyStore(ZkBaseDataAccessor):

    """

    Parameters:
        ZkBaseDataAccessor<T> accessor
        String root
        List<String> subscribedPaths
    """
    def __init__(self, accessor, root, subscribedPaths):
        super(accessor, root, None, subscribedPaths)


    """

    Parameters:
        String zkAddress
        ZkSerializer serializer
        String chrootPath
        List<String> zkCachePaths
    """
    def __init__(self, zkAddress, serializer, chrootPath, zkCachePaths):
        super(zkAddress, serializer, chrootPath, None, zkCachePaths)


    """

    Parameters:
        String zkAddress
        ZkSerializer serializer
        String chrootPath
    """
    def __init__(self, zkAddress, serializer, chrootPath):
        super(zkAddress, serializer, chrootPath, None, None)



