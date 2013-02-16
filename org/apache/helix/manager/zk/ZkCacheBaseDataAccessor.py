# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util import ArrayList
#from java.util import Arrays
#from java.util import Collections
#from java.util import Comparator
#from java.util import List
#from java.util import Map
#from java.util import TreeMap
#from java.util.concurrent import TimeUnit
#from java.util.concurrent.locks import ReentrantLock
#from org.I0Itec.zkclient import DataUpdater
#from org.I0Itec.zkclient import IZkChildListener
#from org.I0Itec.zkclient import IZkDataListener
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.I0Itec.zkclient.serialize import ZkSerializer
#from org.apache.log4j import Logger
#from org.apache.zookeeper.KeeperException import Code
#from org.apache.zookeeper.common import PathUtils
#from org.apache.zookeeper.data import Stat
#from org.apache.zookeeper.server import DataTree
from org.apache.helix.AccessOption import AccessOption
from org.apache.helix.manager.zk.ZkAsyncCallbacks import CreateCallbackHandler
from org.apache.helix.manager.zk.ZkBaseDataAccessor import RetCode
from org.apache.helix.store.HelixPropertyListener import HelixPropertyListener
from org.apache.helix.store.HelixPropertyStore import HelixPropertyStore
from org.apache.helix.store.zk.ZNode import ZNode


# Parameterized type: <T>
class ZkCacheBaseDataAccessor(HelixPropertyStore<T>):

    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    LOG = Logger.getLogger(ZkCacheBaseDataAccessor.class)























    """

    Parameters:
        ZkBaseDataAccessor<T> baseAccessor
        List<String> wtCachePaths
    """
    def __init__(self, baseAccessor, wtCachePaths):
        this(baseAccessor, None, wtCachePaths, None)


    """

    Parameters:
        ZkBaseDataAccessor<T> baseAccessor
        String chrootPath
        List<String> wtCachePaths
        List<String> zkCachePaths
    """
    def __init__(self, baseAccessor, chrootPath, wtCachePaths, zkCachePaths):
        _baseAccessor = baseAccessor
        if chrootPath == None or (chrootPath == "/"): 
            _chrootPath = None
        else:
            PathUtils.validatePath(chrootPath)
            _chrootPath = chrootPath

        _wtCachePaths = wtCachePaths
        _zkCachePaths = zkCachePaths
        _cacheMap = TreeMap<String, Cache<T>>(Comparator<String>() {

            def compare(self, o1, o2):
                """
                Returns int
                Parameters:
                    o1: Stringo2: String
                @Override


                """
                # int
                len1 = o1.split("/").length
                # int
                len2 = o2.split("/").length
                return len1 - len2

        })
        start()


    """

    Parameters:
        String zkAddress
        ZkSerializer serializer
        String chrootPath
        List<String> wtCachePaths
        List<String> zkCachePaths
    """
    def __init__(self, zkAddress, serializer, chrootPath, wtCachePaths, zkCachePaths):
        self._zkclient = ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, serializer)
        self._zkclient.waitUntilConnected(ZkClient.DEFAULT_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)
        self._baseAccessor = ZkBaseDataAccessor<T>(_zkclient)
        if chrootPath == None or (chrootPath == "/"): 
            self._chrootPath = None
        else:
            PathUtils.validatePath(chrootPath)
            self._chrootPath = chrootPath

        self._wtCachePaths = wtCachePaths
        self._zkCachePaths = zkCachePaths
        self._cacheMap = TreeMap<String, Cache<T>>(Comparator<String>() {

            def compare(self, o1, o2):
                """
                Returns int
                Parameters:
                    o1: Stringo2: String
                @Override


                """
                # int
                len1 = o1.split("/").length
                # int
                len2 = o2.split("/").length
                return len1 - len2

        })
        start()


    def prependChroot(self, clientPath):
        """
        Returns String
        Parameters:
            clientPath: String
        Java modifiers:
             private

        """
        if _chrootPath != None: 
            if clientPath.length() == 1: 
                return _chrootPath

            return _chrootPath + clientPath
        else:
            return clientPath



    def prependChroot(self, clientPaths):
        """
        Returns List<String>
        Parameters:
            clientPaths: List<String>
        Java modifiers:
             private

        """
        # List<String>
        serverPaths = ArrayList<String>()
        for # String
        clientPath = None
         in clientPaths) serverPaths.add(prependChroot(clientPath))

        return serverPaths


    def firstCachePath(self, paths):
        """
        Returns String
        Parameters:
            paths: List<String>
        Java modifiers:
             private

        """
        for # String
        cachePath = None
         in _cacheMap.keySet()) for # String
            path = None
             in paths) if path.startsWith(cachePath): 
                    return path



        return None


    def getCache(self, path):
        """
        Returns Cache<T>
        Parameters:
            path: String
        Java modifiers:
             private

        """
        for # String
        cachePath = None
         in _cacheMap.keySet()) if path.startsWith(cachePath): 
                return _cacheMap.get(cachePath)


        return None


    def getCache(self, paths):
        """
        Returns Cache<T>
        Parameters:
            paths: List<String>
        Java modifiers:
             private

        """
        # Cache<T>
        cache = None
        for # String
        path = None
         in paths) for # String
            cachePath = None
             in _cacheMap.keySet()) if cache == None && path.startsWith(cachePath): 
                    cache = _cacheMap.get(cachePath)
                else:
                    if cache != None && cache != _cacheMap.get(cachePath): 
                        raise IllegalArgumentException("Couldn't do cross-cache async operations. paths: " + paths)




        return cache


    def updateCache(self, cache, createPaths, success, updatePath, data, stat):
        """
        Returns void
        Parameters:
            cache: Cache<T>createPaths: List<String>success: booleanupdatePath: Stringdata: Tstat: Stat
        Java modifiers:
             private

        """
        if createPaths == None or createPaths.isEmpty(): 
            if success: 
                cache.update(updatePath, data, stat)

        else:
            # String
            firstPath = firstCachePath(createPaths)
            if firstPath != None: 
                cache.updateRecursive(firstPath)




    def create(self, path, data, options):
        """
        Returns boolean
        Parameters:
            path: Stringdata: Toptions: int
        @Override


        """
        # String
        clientPath = path
        # String
        serverPath = prependChroot(clientPath)
        # Cache<T>
        cache = getCache(serverPath)
        if cache != None: 
            try:
                cache.lockWrite()
                # List<String>
                pathsCreated = ArrayList<String>()
                # RetCode
                rc = _baseAccessor.create(serverPath, data, pathsCreated, options)
                # boolean
                success = (rc == RetCode.OK)
                updateCache(cache, pathsCreated, success, serverPath, data, ZNode.ZERO_STAT)
                return success
            final:
                    cache.unlockWrite()


        return _baseAccessor.create(serverPath, data, options)


    def set(self, path, data, options):
        """
        Returns boolean
        Parameters:
            path: Stringdata: Toptions: int
        @Override


        """
        # String
        clientPath = path
        # String
        serverPath = prependChroot(clientPath)
        # Cache<T>
        cache = getCache(serverPath)
        if cache != None: 
            try:
                cache.lockWrite()
                # Stat
                setStat = Stat()
                # List<String>
                pathsCreated = ArrayList<String>()
                # boolean
                success = _baseAccessor.set(serverPath, data, pathsCreated, setStat, -1, options)
                updateCache(cache, pathsCreated, success, serverPath, data, setStat)
                return success
            final:
                    cache.unlockWrite()


        return _baseAccessor.set(serverPath, data, options)


    def update(self, path, updater, options):
        """
        Returns boolean
        Parameters:
            path: Stringupdater: DataUpdater<T>options: int
        @Override


        """
        # String
        clientPath = path
        # String
        serverPath = prependChroot(clientPath)
        # Cache<T>
        cache = getCache(serverPath)
        if cache != None: 
            try:
                cache.lockWrite()
                # Stat
                setStat = Stat()
                # List<String>
                pathsCreated = ArrayList<String>()
                # T
                updateData = _baseAccessor.update(serverPath, updater, pathsCreated, setStat, options)
                # boolean
                success = (updateData != None)
                updateCache(cache, pathsCreated, success, serverPath, updateData, setStat)
                return success
            final:
                    cache.unlockWrite()


        return _groupCommit.commit(_baseAccessor, options, serverPath, updater)


    def exists(self, path, options):
        """
        Returns boolean
        Parameters:
            path: Stringoptions: int
        @Override


        """
        # String
        clientPath = path
        # String
        serverPath = prependChroot(clientPath)
        # Cache<T>
        cache = getCache(serverPath)
        if cache != None: 
            # boolean
            exists = cache.exists(serverPath)
            if exists: 
                return True


        return _baseAccessor.exists(serverPath, options)


    def remove(self, path, options):
        """
        Returns boolean
        Parameters:
            path: Stringoptions: int
        @Override


        """
        # String
        clientPath = path
        # String
        serverPath = prependChroot(clientPath)
        # Cache<T>
        cache = getCache(serverPath)
        if cache != None: 
            try:
                cache.lockWrite()
                # boolean
                success = _baseAccessor.remove(serverPath, options)
                if success: 
                    cache.purgeRecursive(serverPath)

                return success
            final:
                    cache.unlockWrite()


        return _baseAccessor.remove(serverPath, options)


    def get(self, path, stat, options):
        """
        Returns T
        Parameters:
            path: Stringstat: Statoptions: int
        @Override


        """
        # String
        clientPath = path
        # String
        serverPath = prependChroot(clientPath)
        # Cache<T>
        cache = getCache(serverPath)
        if cache != None: 
            # T
            record = None
            # ZNode
            znode = cache.get(serverPath)
            if znode != None: 
                record = ((T) znode.getData())
                if stat != None: 
                    DataTree.copyStat(znode.getStat(), stat)

                return record
            else:
                try:
                    cache.lockWrite()
                    record = _baseAccessor.get(serverPath, stat, options | AccessOption.THROW_EXCEPTION_IFNOTEXIST)
                    cache.update(serverPath, record, stat)
                except ZkNoNodeException, e:
                    if AccessOption.isThrowExceptionIfNotExist(options): 
                        raise e


                final:
                        cache.unlockWrite()

                return record


        return _baseAccessor.get(serverPath, stat, options)


    def getStat(self, path, options):
        """
        Returns Stat
        Parameters:
            path: Stringoptions: int
        @Override


        """
        # String
        clientPath = path
        # String
        serverPath = prependChroot(clientPath)
        # Cache<T>
        cache = getCache(serverPath)
        if cache != None: 
            # Stat
            stat = Stat()
            # ZNode
            znode = cache.get(serverPath)
            if znode != None: 
                return znode.getStat()
            else:
                try:
                    cache.lockWrite()
                    # T
                    data = _baseAccessor.get(serverPath, stat, options)
                    cache.update(serverPath, data, stat)
                except ZkNoNodeException, e:
                    return None
                final:
                        cache.unlockWrite()

                return stat


        return _baseAccessor.getStat(serverPath, options)


    def createChildren(self, paths, records, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>records: List<T>options: int
        @Override


        """
        # int
        size = paths.size()
        # List<String>
        serverPaths = prependChroot(paths)
        # Cache<T>
        cache = getCache(serverPaths)
        if cache != None: 
            try:
                cache.lockWrite()
                # boolean[]
                needCreate = new boolean[size]
                Arrays.fill(needCreate, True)
                # List<List<String>>
                pathsCreatedList = ArrayList<List<String>>(Collections.nCopies(size, None))
                # CreateCallbackHandler[]
                createCbList = _baseAccessor.create(serverPaths, records, needCreate, pathsCreatedList, options)
                # boolean[]
                success = new boolean[size]
                for (# int
                i = 0; i < size; i++) # CreateCallbackHandler
                    cb = createCbList[i]
                    success[i] = (Code.get(cb.getRc()) == Code.OK)
                    updateCache(cache, pathsCreatedList.get(i), success[i], serverPaths.get(i), records.get(i), ZNode.ZERO_STAT)

                return success
            final:
                    cache.unlockWrite()


        return _baseAccessor.createChildren(serverPaths, records, options)


    def setChildren(self, paths, records, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>records: List<T>options: int
        @Override


        """
        # int
        size = paths.size()
        # List<String>
        serverPaths = prependChroot(paths)
        # Cache<T>
        cache = getCache(serverPaths)
        if cache != None: 
            try:
                cache.lockWrite()
                # List<Stat>
                setStats = ArrayList<Stat>()
                # List<List<String>>
                pathsCreatedList = ArrayList<List<String>>(Collections.nCopies(size, None))
                # boolean[]
                success = _baseAccessor.set(serverPaths, records, pathsCreatedList, setStats, options)
                for (# int
                i = 0; i < size; i++) updateCache(cache, pathsCreatedList.get(i), success[i], serverPaths.get(i), records.get(i), setStats.get(i))

                return success
            final:
                    cache.unlockWrite()


        return _baseAccessor.setChildren(serverPaths, records, options)


    def updateChildren(self, paths, updaters, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>updaters: List<DataUpdater<T>>options: int
        @Override


        """
        # int
        size = paths.size()
        # List<String>
        serverPaths = prependChroot(paths)
        # Cache<T>
        cache = getCache(serverPaths)
        if cache != None: 
            try:
                cache.lockWrite()
                # List<Stat>
                setStats = ArrayList<Stat>()
                # boolean[]
                success = new boolean[size]
                # List<List<String>>
                pathsCreatedList = ArrayList<List<String>>(Collections.nCopies(size, None))
                # List<T>
                updateData = _baseAccessor.update(serverPaths, updaters, pathsCreatedList, setStats, options)
                for (# int
                i = 0; i < size; i++) success[i] = (updateData.get(i) != None)
                    updateCache(cache, pathsCreatedList.get(i), success[i], serverPaths.get(i), updateData.get(i), setStats.get(i))

                return success
            final:
                    cache.unlockWrite()


        return _baseAccessor.updateChildren(serverPaths, updaters, options)


    def exists(self, paths, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>options: int
        @Override


        """
        # int
        size = paths.size()
        # List<String>
        serverPaths = prependChroot(paths)
        # boolean
        exists[] = new boolean[size]
        for (# int
        i = 0; i < size; i++) exists[i] = exists(serverPaths.get(i), options)

        return exists


    def remove(self, paths, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>options: int
        @Override


        """
        # int
        size = paths.size()
        # List<String>
        serverPaths = prependChroot(paths)
        # Cache<T>
        cache = getCache(serverPaths)
        if cache != None: 
            try:
                cache.lockWrite()
                # boolean[]
                success = _baseAccessor.remove(serverPaths, options)
                for (# int
                i = 0; i < size; i++) if success[i]: 
                        cache.purgeRecursive(serverPaths.get(i))


                return success
            final:
                    cache.unlockWrite()


        return _baseAccessor.remove(serverPaths, options)


    def get(self, paths, stats, options):
        """
        Returns List<T>
        Parameters:
            paths: List<String>stats: List<Stat>options: int
        @Override


        """
        if paths == None or paths.isEmpty(): 
            return Collections.emptyList()

        # int
        size = paths.size()
        # List<String>
        serverPaths = prependChroot(paths)
        # List<T>
        records = ArrayList<T>(Collections.nCopies(size, None))
        # List<Stat>
        readStats = ArrayList<Stat>(Collections.nCopies(size, None))
        # boolean
        needRead = False
        # boolean
        needReads[] = new boolean[size]
        # Cache<T>
        cache = getCache(serverPaths)
        if cache != None: 
            try:
                cache.lockRead()
                for (# int
                i = 0; i < size; i++) # ZNode
                    zNode = cache.get(serverPaths.get(i))
                    if zNode != None: 
                        records.set(i, (T) zNode.getData())
                        readStats.set(i, zNode.getStat())
                    else:
                        needRead = True
                        needReads[i] = True


            final:
                    cache.unlockRead()

            if needRead: 
                cache.lockWrite()
                try:
                    # List<T>
                    readRecords = _baseAccessor.get(serverPaths, readStats, needReads)
                    for (# int
                    i = 0; i < size; i++) if needReads[i]: 
                            records.set(i, readRecords.get(i))
                            cache.update(serverPaths.get(i), readRecords.get(i), readStats.get(i))


                final:
                        cache.unlockWrite()


            if stats != None: 
                stats.clear()
                stats.addAll(readStats)

            return records

        return _baseAccessor.get(serverPaths, stats, options)


    def getStats(self, paths, options):
        """
        Returns Stat[]
        Parameters:
            paths: List<String>options: int
        @Override


        """
        # List<String>
        serverPaths = prependChroot(paths)
        return _baseAccessor.getStats(serverPaths, options)


    def getChildNames(self, parentPath, options):
        """
        Returns List<String>
        Parameters:
            parentPath: Stringoptions: int
        @Override


        """
        # String
        serverParentPath = prependChroot(parentPath)
        # Cache<T>
        cache = getCache(serverParentPath)
        if cache != None: 
            # ZNode
            znode = cache.get(serverParentPath)
            if znode != None && znode.getChildSet() != Collections.emptySet(): 
                # List<String>
                childNames = ArrayList<String>(znode.getChildSet())
                Collections.sort(childNames)
                return childNames
            else:
                try:
                    cache.lockWrite()
                    # List<String>
                    childNames = _baseAccessor.getChildNames(serverParentPath, options)
                    cache.addToParentChildSet(serverParentPath, childNames)
                    return childNames
                final:
                        cache.unlockWrite()



        return _baseAccessor.getChildNames(serverParentPath, options)


    def getChildren(self, parentPath, stats, options):
        """
        Returns List<T>
        Parameters:
            parentPath: Stringstats: List<Stat>options: int
        @Override


        """
        # List<String>
        childNames = getChildNames(parentPath, options)
        if childNames == None: 
            return None

        # List<String>
        paths = ArrayList<String>()
        for # String
        childName = None
         in childNames) # String
            path = parentPath + "/" + childName
            paths.add(path)

        return get(paths, stats, options)


    def subscribeDataChanges(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: IZkDataListener
        @Override


        """
        # String
        serverPath = prependChroot(path)
        _baseAccessor.subscribeDataChanges(serverPath, listener)


    def unsubscribeDataChanges(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: IZkDataListener
        @Override


        """
        # String
        serverPath = prependChroot(path)
        _baseAccessor.unsubscribeDataChanges(serverPath, listener)


    def subscribeChildChanges(self, path, listener):
        """
        Returns List<String>
        Parameters:
            path: Stringlistener: IZkChildListener
        @Override


        """
        # String
        serverPath = prependChroot(path)
        return _baseAccessor.subscribeChildChanges(serverPath, listener)


    def unsubscribeChildChanges(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: IZkChildListener
        @Override


        """
        # String
        serverPath = prependChroot(path)
        _baseAccessor.unsubscribeChildChanges(serverPath, listener)


    def subscribe(self, parentPath, listener):
        """
        Returns void
        Parameters:
            parentPath: Stringlistener: HelixPropertyListener
        @Override


        """
        # String
        serverPath = prependChroot(parentPath)
        _zkCache.subscribe(serverPath, listener)


    def unsubscribe(self, parentPath, listener):
        """
        Returns void
        Parameters:
            parentPath: Stringlistener: HelixPropertyListener
        @Override


        """
        # String
        serverPath = prependChroot(parentPath)
        _zkCache.unsubscribe(serverPath, listener)


    def start(self):
        """
        Returns void
        @Override


        """
        LOG.info("START: Init ZkCacheBaseDataAccessor: " + _chrootPath + ", " + _wtCachePaths + ", " + _zkCachePaths)
        try:
            _eventLock.lockInterruptibly()
            if _eventThread != None: 
                LOG.warn(_eventThread + " has already started")
            else:
                if _zkCachePaths == None or _zkCachePaths.isEmpty(): 
                    LOG.warn("ZkCachePaths is null or empty. Will not start ZkCacheEventThread")
                else:
                    LOG.debug("Starting ZkCacheEventThread...")
                    _eventThread = ZkCacheEventThread("")
                    _eventThread.start()


        except InterruptedException, e:
            LOG.error("Current thread is interrupted when starting ZkCacheEventThread. "+ str(e))
        final:
                _eventLock.unlock()

        LOG.debug("Start ZkCacheEventThread...done")
        _wtCache = WriteThroughCache<T>(_baseAccessor, _wtCachePaths)
        _zkCache = ZkCallbackCache<T>(_baseAccessor, _chrootPath, _zkCachePaths, _eventThread)
        if _wtCachePaths != None && not _wtCachePaths.isEmpty(): 
            for # String
            path = None
             in _wtCachePaths) _cacheMap.put(path, _wtCache)


        if _zkCachePaths != None && not _zkCachePaths.isEmpty(): 
            for # String
            path = None
             in _zkCachePaths) _cacheMap.put(path, _zkCache)




    def stop(self):
        """
        Returns void
        @Override


        """
        try:
            _eventLock.lockInterruptibly()
            if _zkclient != None: 
                _zkclient.close()
                _zkclient = None

            if _eventThread == None: 
                LOG.warn(_eventThread + " has already stopped")
                return

            LOG.debug("Stopping ZkCacheEventThread...")
            _eventThread.interrupt()
            _eventThread.join(2000)
            _eventThread = None
        except InterruptedException, e:
            LOG.error("Current thread is interrupted when stopping ZkCacheEventThread.")
        final:
                _eventLock.unlock()

        LOG.debug("Stop ZkCacheEventThread...done")


    def reset(self):
        """
        Returns void
        @Override


        """
        if _wtCache != None: 
            _wtCache.reset()

        if _zkCache != None: 
            _zkCache.reset()




