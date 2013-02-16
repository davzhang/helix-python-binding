# package org.apache.helix.store
#from org.apache.helix.store import *
#from org.apache.log4j import Logger
from org.apache.helix.manager.zk.ByteArraySerializer import ByteArraySerializer
from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.store.file.FilePropertyStore import FilePropertyStore
from org.apache.helix.store.zk.ZKPropertyStore import ZKPropertyStore


class PropertyStoreFactory:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(PropertyStoreFactory.class)

    def getZKPropertyStore(zkAddress, serializer, rootNamespace):
        """
        Returns PropertyStore<T>
        Parameters:
            zkAddress: Stringserializer: PropertySerializer<T>rootNamespace: String
        Java modifiers:
             static

        Parameterized: <T extends Object> 
        """
        if zkAddress == None or serializer == None or rootNamespace == None: 
            raise IllegalArgumentException("zkAddress|serializer|rootNamespace can't be null")


        LOG.info("Get a zk property store. zkAddr: " + zkAddress + ", root: " + rootNamespace)
        # ZkClient
        zkClient = ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, ByteArraySerializer())
        return ZKPropertyStore<T>(zkClient, serializer, rootNamespace)


    def getFilePropertyStore(serializer, rootNamespace, comparator):
        """
        Returns PropertyStore<T>
        Parameters:
            serializer: PropertySerializer<T>rootNamespace: Stringcomparator: PropertyJsonComparator<T>
        Java modifiers:
             static

        Parameterized: <T extends Object> 
        """
        if comparator == None or serializer == None or rootNamespace == None: 
            raise IllegalArgumentException("arguments can't be null")


        LOG.info("Get a file property store. root: " + rootNamespace)
        # FilePropertyStore<T>
        store = FilePropertyStore<T>(serializer, rootNamespace, comparator)
        return store



