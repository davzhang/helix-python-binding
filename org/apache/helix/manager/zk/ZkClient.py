# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util import Arrays
#from java.util import List
#from java.util.concurrent import Callable
#from org.I0Itec.zkclient import IZkConnection
#from org.I0Itec.zkclient import ZkConnection
#from org.I0Itec.zkclient.exception import ZkException
#from org.I0Itec.zkclient.exception import ZkInterruptedException
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.I0Itec.zkclient.serialize import SerializableSerializer
#from org.I0Itec.zkclient.serialize import ZkSerializer
#from org.apache.log4j import Logger
#from org.apache.zookeeper import CreateMode
#from org.apache.zookeeper import KeeperException
#from org.apache.zookeeper.ZooDefs import Ids
#from org.apache.zookeeper.data import Stat
import sys
import traceback
from kazoo.exceptions import NoNodeException, NodeExistsError, NodeExistsException
import os
from org.apache.helix.manager.zk import ChainedPathZkSerializer
from org.apache.helix.manager.zk.BasicZkSerializer import BasicZkSerializer
from org.apache.helix.manager.zk.ByteArraySerializer import ByteArraySerializer
from org.apache.helix.manager.zk.ZNRecordStreamingSerializer import ZNRecordStreamingSerializer
from org.apache.helix.manager.zk.ZkAsyncCallbacks import CreateCallbackHandler
from org.apache.helix.manager.zk.ZkAsyncCallbacks import DeleteCallbackHandler
from org.apache.helix.manager.zk.ZkAsyncCallbacks import ExistsCallbackHandler
#from org.apache.helix.manager.zk.ZkAsyncCallbacks import GetDataCallbackHandler
from org.apache.helix.manager.zk.ZkAsyncCallbacks import SetDataCallbackHandler
from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.ZKConstants import CreateMode

from org.apache.helix.util.logger import get_logger

from kazoo.client import KazooClient
from org.apache.helix.util.ZKConstants import HelixZNodeStat

#class ZkClient(org.I0Itec.zkclient.ZkClient):
from org.apache.helix.util.misc import ternary

#class ZkClient(KazooClient):
'''
 ZkClient is operating on ZNRecord
'''
class ZkClient():


    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)
    """
    Java modifiers:
         final static
    Type:
        int
    """
    DEFAULT_CONNECTION_TIMEOUT = 60 * 1000

    """
    Java modifiers:
         final static
    Type:
        int
    """
    DEFAULT_SESSION_TIMEOUT = 30 * 1000



#    """
#
#    Parameters:
#        IZkConnection connection
#        int connectionTimeout
#        PathBasedZkSerializer zkSerializer
#    """
#    def __init__(self, connection, connectionTimeout=DEFAULT_SESSION_TIMEOUT, zkSerializer=ByteArraySerializer()):
##        super(connection, connectionTimeout, ByteArraySerializer())
#        self._zkSerializer = zkSerializer
#        # StackTraceElement[]
##        calls = Thread.currentThread().getStackTrace()
##        calls = traceback.print_stack()
#        LOG.info("create a new zkclient. " + repr(traceback.extract_stack()))
#
#
#    """1
#
#    Parameters:
#        IZkConnection connection
#        int connectionTimeout
#        ZkSerializer zkSerializer
#    """
#    def __init__(self, connection, connectionTimeout, zkSerializer):
#        this(connection, connectionTimeout, BasicZkSerializer(zkSerializer))
#
#
#    """
#
#    Parameters:
#        IZkConnection connection
#        int connectionTimeout
#    """
#    def __init__(self, connection, connectionTimeout=sys.maxint):
#        this(connection, connectionTimeout, SerializableSerializer())
#
#
#    """
#
#    Parameters:
#        String zkServers
#        int sessionTimeout
#        int connectionTimeout
#        ZkSerializer zkSerializer
#    """
#    def __init__(self, zkServers, sessionTimeout, connectionTimeout, zkSerializer):
#        this.__init__((zkServers, sessionTimeout), connectionTimeout, zkSerializer)
#
#
    """

    Parameters:
        String zkServers
        int sessionTimeout
        int connectionTimeout
        PathBasedZkSerializer zkSerializer
    """
    # DEFAULT_ZK_SERIALIZER = ChainedPathZkSerializer.builder(ZNRecordStreamingSerializer()).serialize(propertyStorePath, ByteArraySerializer()).build()
    # TODO: more serilizer?
    def __init__(self, zkServers, sessionTimeout=DEFAULT_SESSION_TIMEOUT, connectionTimeout=DEFAULT_CONNECTION_TIMEOUT, zkSerializer=BasicZkSerializer):
        self._connection = KazooClient(hosts=zkServers, timeout=sessionTimeout)
        self._zkSerializer = zkSerializer
        self._connection.start(connectionTimeout)
        self.LOG.info("create a new zkclient. " + repr(traceback.extract_stack()))

#        this(ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer)
#
#
#    """
#
#    Parameters:
#        String zkServers
#        int sessionTimeout
#        int connectionTimeout
#    """
#    def __init__(self, zkServers, sessionTimeout, connectionTimeout):
#        this(ZkConnection(zkServers, sessionTimeout), connectionTimeout, SerializableSerializer())
#
#
#    """
#
#    Parameters:
#        String zkServers
#        int connectionTimeout
#    """
#    def __init__(self, zkServers, connectionTimeout):
#        this(ZkConnection(zkServers), connectionTimeout, SerializableSerializer())
#
#
#    """
#
#    Parameters:
#        String zkServers
#    """
#    def __init__(self, zkServers):
#        this(ZkConnection(zkServers), Integer.MAX_VALUE, SerializableSerializer())
#
#
#    static

    def setZkSerializer(self, zkSerializer):
        """
        Returns void
        Parameters:
            zkSerializer: ZkSerializer
        @Override


        """
        _zkSerializer = BasicZkSerializer(zkSerializer)


    def setZkSerializer(self, zkSerializer):
        """
        Returns void
        Parameters:
            zkSerializer: PathBasedZkSerializer


        """
        _zkSerializer = zkSerializer


    def getConnection(self):
        """
        Returns IZkConnection


        """
        return self._connection


    def close(self):
        """
        Returns void
        @Override


        Throws: 
            ZkInterruptedException
        """
        # StackTraceElement[]
#        calls = Thread.currentThread().getStackTrace()
        self.LOG.info("closing a zkclient. zookeeper: " + repr(self._connection) + ", callStack: " + traceback.extract_stack())
        if self._connection:
            self._connection.close()


    def getStat(self, path):
        """
        Returns Stat
        Parameters:
            path: String


        """
        stat = self._connection.exists(path)
        return stat
        # long
#        startT = System.nanoTime()
#        try:
#            # Stat
#            stat = retryUntilConnected(Callable<Stat>() {
#
#                def call(self):
#                    """
#                    Returns Stat
#                    @Override
#
#
#                    Throws:
#                        Exception
#                    """
#                    # Stat
#                    stat = ((ZkConnection) self._connection).getZookeeper().exists(path, False)
#                    return stat
#
#            })
#            return stat
#        final:
#                # long
#                endT = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("exists, path: " + str(path)+ ", time: " + str((endT - startT) )+ " ns")

#    def hasListeners(self,path):
#        ''' given a path, find out if there is listener for the path
#        '''
#        ret = False
#        for childWatcher in self._connection._child_watchers:
#           if childWatcher.

    def exists(self, path, watch=None):
        """
        Returns boolean
        Parameters:
            path: Stringwatch: boolean
        @Override
        Java modifiers:
             protected

        """
        # long
#        if not watch:
#            watch = path in self._connection._child_watchers or path in self._connection._data_watchers
        # in java, watch is a boolean
        # in kazoo, it is a call back function

#        stat = self._connection.exists(path, watch)
        stat = self._connection.exists(path)
        return stat
#        startT = System.nanoTime()
#        try:
#            return retryUntilConnected(Callable<Boolean>() {
#
#                def call(self):
#                    """
#                    Returns Boolean
#                    @Override
#
#
#                    Throws:
#                        Exception
#                    """
#                    return self._connection.exists(path, watch)
#
#            })
#        final:
#                # long
#                endT = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("exists, path: " + str(path)+ ", time: " + str((endT - startT) )+ " ns")




    def getChildren(self, path, watch=None):
        """
        Returns List<String>
        Parameters:
            path: Stringwatch: boolean
        @Override
        Java modifiers:
             protected

        """
        return self._connection.get_children(path, watch)
#        # long
#        startT = System.nanoTime()
#        try:
#            return retryUntilConnected(Callable<List<String>>() {
#
#                def call(self):
#                    """
#                    Returns List<String>
#                    @Override
#
#
#                    Throws:
#                        Exception
#                    """
#                    return self._connection.getChildren(path, watch)
#
#            })
#        final:
#                # long
#                endT = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("getChildren, path: " + str(path)+ ", time: " + str((endT - startT) )+ " ns")




    def deserialize(self, data, path):
        """
        Returns T
        Parameters:
            data: byte[]path: String
        # Annotation: @SuppressWarnings("unchecked")



        Parameterized: <T extends Object> 
        """
        if data == None: 
            return None

        return self._zkSerializer.deserialize(data, path)

    def copyStat(self, src, dest):
        for fieldName in src._fields:
            setattr(dest, fieldName, getattr(src, fieldName))

    def readData(self, *args):
        if len(args)==3 and isinstance(args[1], HelixZNodeStat) and isinstance(args[2], bool):
            return self.readDataAndStat(*args)
        elif len(args)==2 and isinstance(args[1],bool):  # readData(path, nullIfNoExist )
            return self.readDataAndStat(args[0], HelixZNodeStat(), args[1])
        elif len(args)>=2 and isinstance(args[1], HelixZNodeStat):
            return self.readDataStatInternal(*args)
        else:
            raise IllegalArgumentException("Wrong args: %s" % args)

    def readDataStatInternal(self, path, stat, watch=None):
        """
        Returns T
        Parameters:
            path: Stringstat: Statwatch: boolean
        @Override
        # Annotation: @SuppressWarnings("unchecked")

        Java modifiers:
             protected

        Parameterized: <T extends Object> 
        """
        data, statRet = self._connection.get(path, watch)
        self.copyStat(statRet, stat)   # copy over the stats
        return self.deserialize(data, path)
#
#        # long
#        startT = System.nanoTime()
#        try:
#            # byte[]
#            data = retryUntilConnected(Callable<byte[]>() {
#
#                def call(self):
#                    """
#                    Returns byte[]
#                    @Override
#
#
#                    Throws:
#                        Exception
#                    """
#                    return self._connection.readData(path, stat, watch)
#
#            })
#            return (T) deserialize(data, path)
#        final:
#                # long
#                endT = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("getData, path: " + str(path)+ ", time: " + str((endT - startT) )+ " ns")
#
#


    def readDataAndStat(self, path, stat, returnNullIfPathNotExists):
        """
        Returns T
        Parameters:
            path: Stringstat: StatreturnNullIfPathNotExists: boolean
        # Annotation: @SuppressWarnings("unchecked")



        Parameterized: <T extends Object> 
        """
        # T
        data = None
        try:
            data = self.readDataStatInternal(path, stat)
        except NoNodeException, e:
            if not returnNullIfPathNotExists: 
                raise e
        return data


    def getServers(self):
        """
        Returns String


        """
        return self._connection.hosts


    def serialize(self, data, path):
        """
        Returns byte[]
        Parameters:
            data: Objectpath: String


        """
        return self._zkSerializer.serialize(data, path)


    def writeData(self, path, data, expectedVersion):
        """
        Returns void
        Parameters:
            path: Stringdatat: ObjectexpectedVersion: int
        @Override


        """
        bytes = self._zkSerializer.serialize(data, path)
        self._connection.set(path, bytes, expectedVersion)
#        # long
#        startT = System.nanoTime()
#        try:
#            # byte[]
#            data = serialize(datat, path)
#            retryUntilConnected(Callable<Object>() {
#
#                def call(self):
#                    """
#                    Returns Object
#                    @Override
#
#
#                    Throws:
#                        Exception
#                    """
#                    self._connection.writeData(path, data, expectedVersion)
#                    return None
#
#            })
#        final:
#                # long
#                endT = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("setData, path: " + str(path)+ ", time: " + str((endT - startT) )+ " ns")
#
#


    def writeDataGetStat(self, path, datat, expectedVersion):
        """
        Returns Stat
        Parameters:
            path: Stringdatat: ObjectexpectedVersion: int


        Throws: 
            InterruptedException
        """
        bytes = self._zkSerializer.serialize(datat, path)
        stat = self._connection.set(path, bytes, expectedVersion)
        statRet = HelixZNodeStat()
        self.copyStat(stat, statRet)   # copy over the stats
        return statRet

#    # Stat
#        stat = None
#        # long
#        start = System.nanoTime()
#        try:
#            # byte[]
#            bytes = _zkSerializer.serialize(datat, path)
#            stat = ((ZkConnection) self._connection).getZookeeper().setData(path, bytes, expectedVersion)
#            return stat
#        except KeeperException, e:
#            raise ZkException.create(e)
#
#        final:
#                # long
#                end = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("setData, path: " + str(path)+ ", time: " + str((end - start) )+ " ns")




    def create(self, path, data, mode):
        """
        Returns String
        Parameters:
            path: Stringdata: Objectmode: CreateMode
        @Override


        Throws: 
            ZkInterruptedException
            IllegalArgumentException
            ZkException
            RuntimeException
        """
        if path == None: 
            raise BaseException("path must not be null.")
#        try:
        if data: bytes = self.serialize(data, path)
        else: bytes = b""
        realPath = self._connection.create(path, bytes, None, CreateMode.isEphemeral(mode), CreateMode.isSequential(mode))
        return realPath
#        except: Exception e
#            ra
#        finally: pass

#
#        # long
#        startT = System.nanoTime()
#        try:
#            # byte[]
#            bytes = java2python_runtime.ternary(data == None, None, serialize(data, path))
#            return retryUntilConnected(Callable<String>() {
#
#                def call(self):
#                    """
#                    Returns String
#                    @Override
#
#
#                    Throws:
#                        Exception
#                    """
#                    return self._connection.create(path, bytes, mode)
#
#            })
#        final:
#                # long
#                endT = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("create, path: " + str(path)+ ", time: " + str((endT - startT) )+ " ns")




    def delete(self, path):
        """
        Returns boolean
        Parameters:
            path: String
        @Override


        """
        return self._connection.delete(path)
#        # long
#        startT = System.nanoTime()
#        try:
#            try:
#                retryUntilConnected(Callable<Object>() {
#
#                    def call(self):
#                        """
#                        Returns Object
#                        @Override
#
#
#                        Throws:
#                            Exception
#                        """
#                        self._connection.delete(path)
#                        return None
#
#                })
#                return True
#            except ZkNoNodeException, e:
#                return False
#
#        final:
#                # long
#                endT = System.nanoTime()
#                if LOG.isDebugEnabled():
#                    LOG.debug("delete, path: " + str(path)+ ", time: " + str((endT - startT) )+ " ns")
#



    def asyncCreate(self, path, datat, mode, cb):
        """
        Returns void
        Parameters:
            path: Stringdatat: Objectmode: CreateModecb: CreateCallbackHandler


        """
        data = b""
        if datat: data = self.serialize(datat, path)
        asyncObj = self._connection.create_async(path, data, None, CreateMode.isEphemeral(mode), CreateMode.isSequential(mode))
        asyncObj.rawlink(cb.callback)

#        # byte[]
#        data = None
#        if datat != None:
#            data = serialize(datat, path)
#
#        ((ZkConnection) self._connection).getZookeeper().create(path, data, Ids.OPEN_ACL_UNSAFE, mode, cb, None)


    def asyncSetData(self, path, datat, version, cb):
        """
        Returns void
        Parameters:
            path: Stringdatat: Objectversion: intcb: SetDataCallbackHandler


        """
        # byte[]
        data = self.serialize(datat, path)
        asyncObj = self._connection.set_async(path, data)
        asyncObj.rawlink(cb.callback)
#        ((ZkConnection) self._connection).getZookeeper().setData(path, data, version, cb, None)


    def asyncGetData(self, path, cb):
        """
        Returns void
        Parameters:
            path: Stringcb: GetDataCallbackHandler
        """

        asyncObj = self._connection.get_async(path)
        asyncObj.rawlink(cb.callback)


#        ((ZkConnection) self._connection).getZookeeper().getData(path, None, cb, None)


    def asyncExists(self, path, cb):
        """
        Returns void
        Parameters:
            path: Stringcb: ExistsCallbackHandler


        """
        asyncObj = self._connection.exists_async(path)
        asyncObj.rawlink(cb.callback)

#    ((ZkConnection) self._connection).getZookeeper().exists(path, None, cb, None)


    def asyncDelete(self, path, cb):
        """
        Returns void
        Parameters:
            path: Stringcb: DeleteCallbackHandler


        """
        asyncObj = self._connection.delete_async(path)
        asyncObj.rawlink(cb.callback)

#    ((ZkConnection) self._connection).getZookeeper().delete(path, -1, cb, None)


    # format of session id to hex
    def getSessionId(self):
        return str(hex(self._connection._session_id))

    #
    # java zk methods. Emulate teh ZkClient.java
    #
    def subscribeStateChanges(self, zkStateChangeListener):
        self._connection.add_listener(zkStateChangeListener.handleStateChanged)

    def waitUntilConnected(self, timeout, unit):
        # do not wait, just return connect
        return self._connection.connected

    def createPersistent(self,path, createParents=False):
        try:
            self.create(path, b"", CreateMode.PERSISTENT)
        except NodeExistsException, e:
            if not createParents: raise e
        except NoNodeException, e:
            if not createParents: raise e
            parentDir = os.path.dirname(path)
            self.createPersistent(parentDir, createParents)
            self.createPersistent(path, createParents)


    def deleteRecursive(self, path):
#        List<String> children;
        children = []
        try :
            children = self.getChildren(path)
        except NoNodeException, e:
            return True

        for subPath in children:
            if not self.deleteRecursive(path + "/" + subPath):
                return False
        return self.delete(path)

