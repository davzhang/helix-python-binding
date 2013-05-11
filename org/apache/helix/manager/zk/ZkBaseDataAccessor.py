# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import File
#from java.util import ArrayList
#from java.util import Arrays
#from java.util import Collections
#from java.util import Iterator
#from java.util import List
#from org.I0Itec.zkclient import DataUpdater
#from org.I0Itec.zkclient import IZkChildListener
#from org.I0Itec.zkclient import IZkDataListener
#from org.I0Itec.zkclient.exception import ZkBadVersionException
#from org.I0Itec.zkclient.exception import ZkException
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.I0Itec.zkclient.exception import ZkNodeExistsException
#from org.apache.log4j import Logger
#from org.apache.zookeeper import CreateMode
#from org.apache.zookeeper.KeeperException import Code
#from org.apache.zookeeper.data import Stat
#from org.apache.zookeeper.server import DataTree
import copy
from kazoo.exceptions import NoNodeException, NodeExistsException, BadVersionException, ZookeeperError
from kazoo.protocol.states import ZnodeStat, KeeperState
import time
from org.apache.helix.AccessOption import AccessOption
from org.apache.helix.BaseDataAccessor import BaseDataAccessor
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.manager.zk.ZkAsyncCallbacks import CreateCallbackHandler
from org.apache.helix.manager.zk.ZkAsyncCallbacks import DeleteCallbackHandler
from org.apache.helix.manager.zk.ZkAsyncCallbacks import ExistsCallbackHandler
from org.apache.helix.manager.zk.ZkAsyncCallbacks import GetDataCallbackHandler
from org.apache.helix.manager.zk.ZkAsyncCallbacks import SetDataCallbackHandler
from org.apache.helix.store.zk.ZNode import ZNode
from org.apache.helix.util.UserExceptions import IllegalArgumentException

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum, ternary
import os.path

from org.apache.helix.util.ZKConstants import HelixZNodeStat

RetCode=enum('OK', 'NODE_EXISTS', 'ERROR')

# Parameterized type: <T>
class ZkBaseDataAccessor(BaseDataAccessor):


    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)



    """

    Parameters:
        ZkClient zkClient
    """
    def __init__(self, zkClient):
        self._zkClient = zkClient

    def create(self, *args):
        if len(args)==3 and isinstance(args[1], ZNRecord):
            return self.create_path(args[0], args[1], None, args[2]) == RetCode.OK
        elif len(args)==4 and (isinstance(args[1], ZNRecord) or args[1]==None):
            return self.create_path(*args) == RetCode.OK
        elif len(args)==5 and isinstance(args[3], list):
            return self.create_paths(*args)
        else:
            import pdb; pdb.set_trace()
            raise IllegalArgumentException("Wrong args: %s" % str(args))

#    def create(self, path, record, options):
#        """
#        Returns boolean
#        Parameters:
#            path: Stringrecord: Toptions: int
#        @Override
#
#
#        """
#        return self.create(path, record, None, options) == RetCode.OK


    def create_path(self, path, record, pathCreated, options):
        """
        Returns RetCode
        Parameters:
            path: Stringrecord: TpathCreated: List<String>options: int


        """
        # CreateMode
        mode = AccessOption.getMode(options)
        if mode == None: 
            self.LOG.error("Invalid create mode. options: " + str(options))
            return RetCode.ERROR

        # boolean
        retry = True
        
        while retry:
            retry = False
            try:
                self._zkClient.create(path, record, mode)
                if pathCreated != None: 
                    pathCreated.add(path)
                return RetCode.OK
            except NoNodeException, e:
                # String
                parentPath = os.path.abspath(os.path.join(path, os.path.pardir))
#                parentPath = File(path).getParent()
                try:
                    # RetCode
                    rc = self.create(parentPath, None, pathCreated, AccessOption.PERSISTENT)
                    if rc == RetCode.OK or rc == RetCode.NODE_EXISTS: 
                        retry = True
                # TODO:
#                except Exception, e1:
                except KeyboardInterrupt, e1:
                    self.LOG.error("Exception while creating path: " + str(parentPath)+ str(e1))
                    return RetCode.ERROR

            except NodeExistsException, e:
                self.LOG.warn("Node already exists. path: " + path)
                return RetCode.NODE_EXISTS
                # let's it raised the exception
            # TODO:
#            except Exception, e:
            except KeyboardInterrupt, e:
                self.LOG.error("Exception while creating path: " + str(path)+ str(e))
                return RetCode.ERROR

        return RetCode.OK


    def set(self, *args):
        if len(args) == 3:
            return self.set_path(args[0], args[1], None, None, -1, args[2])
        elif len(args)==5 and isinstance(args[0],list):
            return self.set_paths(*args)
        elif len(args)==6 and isinstance(args[0],str):
            return self.set_path(*args)
        else:
            raise IllegalArgumentException("Wrong args: %s" % args)

#    def set(self, path, record, options):
#        """
#        Returns boolean
#        Parameters:
#            path: Stringrecord: Toptions: int
#        @Override
#
#
#        """
#        return set(path, record, None, None, -1, options)


    def set_path(self, path, record, pathsCreated, setstat, expectVersion, options):
        """
        Returns boolean
        Parameters:
            path: Stringrecord: TpathsCreated: List<String>setstat: StatexpectVersion: intoptions: int


        """
        # CreateMode
        mode = AccessOption.getMode(options)
        if mode == None: 
            self.LOG.error("Invalid set mode. options: " + str(options))
            return False

        # boolean
        retry = True
        
        while retry:
            retry = False
            try:
                # Stat
                setStat = self._zkClient.writeDataGetStat(path, record, expectVersion)
                if setstat != None:
                    setstat = copy.copy(setStat)
#                    DataTree.copyStat(setStat, setstat)
            except NoNodeException, e:
#                try:
                    # RetCode
                    rc = self.create(path, record, pathsCreated, options)
                    if rc == RetCode.OK: pass
                    elif rc == RetCode.NODE_EXISTS:
                            retry = True
                    else:
                        self.LOG.error("Fail to set path by creating: " + str(path))
                        return False
                    # TODO: enable the catch?
#                except Exception, e1:
#                    self.LOG.error("Exception while setting path by creating: " + str(path)+ str(e1))
#                    return False

            except BadVersionException, e:
                raise e
            # TODO: enable this?
#            except Exception, e:
#                self.LOG.error("Exception while setting path: " + str(path)+ str(e))
#                return False
        return True


    def update(self, *args):
        if len(args) == 3:
            return self.update_path(args[0], args[1], None, None, args[2])
        elif len(args) == 5 and isinstance(args[0], str):
            return self.update_path(*args)
        elif len(args) == 5 and isinstance(args[0], list):
            return self.update_paths(*args)
        else:
            raise IllegalArgumentException("Wrong args: %s" % args)

#    def update(self, path, updater, options):
#        """
#        Returns boolean
#        Parameters:
#            path: Stringupdater: DataUpdater<T>options: int
#        @Override
#
#
#        """
#        return self.update(path, updater, None, None, options) != None


    def update_path(self, path, updater, createPaths, stat, options):
        """
        Returns T
        Parameters:
            path: Stringupdater: DataUpdater<T>createPaths: List<String>stat: Statoptions: int


        """
        # CreateMode
        mode = AccessOption.getMode(options)
        if mode == None: 
            self.LOG.error("Invalid update mode. options: " + str(options))
            return None

        # boolean
        retry = True
        
        # T
        updatedData = None
        while retry:
            retry = False
            try:
                # Stat
                readStat = HelixZNodeStat()
                # T
                oldData = self._zkClient.readData(path, readStat)
                # T
                newData = updater.update(oldData)
                # Stat
                setStat = self._zkClient.writeDataGetStat(path, newData, readStat.getVersion())
                if stat != None:
                    stat = copy.copy(setStat)
#                    DataTree.copyStat(setStat, stat)

                updatedData = newData
            except BadVersionException, e:
                retry = True
            except NoNodeException, e:
                try:
                    # T
                    newData = updater.update(None)
                    # RetCode
                    rc = self.create(path, newData, createPaths, options)
                    if rc == RetCode.OK:
                        updatedData = newData
                    elif rc == RetCode.NODE_EXISTS:
                        retry = True
                    else:
                        self.LOG.error("Fail to update path by creating: " + str(path))
                        return None
#                except Exception, e1:
                # TODO
                except KeyboardInterrupt, e1:
                    self.LOG.error("Exception while updating path by creating: " + str(path)+ str(e1))
                    return None
        # TODO: revert to the exception handling
#            except Exception, e:
            except KeyboardInterrupt, e:
                self.LOG.error("Exception while updating path: " + str(path)+ str(e))
                return None

        return updatedData


    def get(self, path, stat, options):
        """
        Returns T
        Parameters:
            path: Stringstat: Statoptions: int
        @Override


        """
        # T
        if isinstance(path,list):
            if isinstance(options,list):
                return self.getList(path, stat, options)
            else:
                return self.getMulti(path, stat, options)

        data = None
        try:
            data = self._zkClient.readData(path, stat)
        except NoNodeException, e:
            if AccessOption.isThrowExceptionIfNotExist(options): 
                raise e



        return data


    def getMulti(self, paths, stats, options):
        """
        Returns List<T>
        Parameters:
            paths: List<String>stats: List<Stat>options: int
        @Override


        """
        # boolean[]
        needRead = [True for path in paths]
#        needRead = new boolean[paths.__len__()]
#        Arrays.fill(needRead, True)
        return self.get(paths, stats, needRead)


    def getList(self, paths, stats, needRead):
        """
        Returns List<T>
        Parameters:
            paths: List<String>stats: List<Stat>needRead: boolean[]


        """
        if paths == None or len(paths) == 0:
            return []
#            return Collections.emptyList()

        # clear but save the location, do not create a new one as this one is used in the return
        if stats != None:
            for i in range(len(stats)): stats[i]=None
#            stats = [None for path in paths]
#            stats.clear()
#            stats.addAll(Collections.nCopies(paths.size(), None))

        # long
#        startT = System.nanoTime()
        startT = time.time()
        try:
            # GetDataCallbackHandler[]
            cbList = [None for p in paths]
#            cbList = new GetDataCallbackHandler[paths.__len__()]
            for i in range(len(paths)):
                if not needRead[i]:
                    continue
                path = paths[i]
                cbList[i] = GetDataCallbackHandler()
                self._zkClient.asyncGetData(path, cbList[i])

            for i in range(len(cbList)):
                if not needRead[i]:
                    continue
                # GetDataCallbackHandler
                cb = cbList[i]
                cb.waitForSuccess()

            # List<T>
#            records = ArrayList<T>(Collections.nCopies(paths.__len__(), None))
            records = [None for p in paths]
            for i in range(len(paths)):
                if not needRead[i]:
                    continue
                # GetDataCallbackHandler
                cb = cbList[i]
#                if Code.get(cb.getRc()) == Code.OK:
                if cb.isSuccessful():
                    # T
                    record = self._zkClient.deserialize(cb._data, paths[i])
                    records[i] = record
                    if stats != None: 
                        stats[i] = cb._stat

            return records
        finally:
                # long
                endT = time.time()
#                if self.LOG.isDebugEnabled():
                self.LOG.debug("getData_async, size: " + str(len(paths))+ ", paths: " + str(paths[0])+ ",... time: " + str(str(endT - startT) )+ " ns")




    def removeChildrenWithoutStats(self, stats, records):
        # need to do the reverse
        noStats = [i for i in range(len(stats)) if not stats[i]]
        noStats.reverse()
        for i in noStats:
            del stats[i]
            del records[i]
#        newRecords = []
#        newStats = []
#        for i in range(len(stats)):
#            if stats[i]:
#                newRecords.append(records[i])
#                newStats.append(stats[i])
#        return (newStats, newRecords)

    def getChildren(self, parentPath, stats, options):
        """
        Returns List<T>
        Parameters:
            parentPath: Stringstats: List<Stat>options: int
        @Override


        """
        try:
            # List<String>
            childNames = self.getChildNames(parentPath, options)
            if not childNames:
                return []

            # List<String>
            paths = [parentPath + "/" + childName for childName in childNames]
#            for childName in childNames: # String
#                path = parentPath + "/" + childName
#                paths.append(path)

            # List<Stat>
#            curStats = ArrayList<Stat>(paths.__len__())
            curStats = [None for path in paths]
            # List<T>
            records = self.get(paths, curStats, options)
            # Iterator<T>
#            recordIter = records.iterator()
            # Iterator<Stat>
#            statIter = curStats.iterator()

#            (stat, records) = self.removeChildrenWithoutStats(stat, records)
            self.removeChildrenWithoutStats(curStats, records)

#            while (statIter.hasNext():
#                recordIter.next()
#                if statIter.next() == None:
#                    statIter.remove()
#                    recordIter.remove()


            if stats != None:
                for i in range(len(stats)): stats[i] = curStats[i]
#                stats = copy.copy(curStats)
#                stats.clear()
#                stats.addAll(curStats)

            return records
        except NoNodeException, e:
            return []



    def getChildNames(self, parentPath, options):
        """
        Returns List<String>
        Parameters:
            parentPath: Stringoptions: int
        @Override


        """
        try:
            # List<String>
            childNames = sorted(self._zkClient.getChildren(parentPath))
            return childNames
        except NoNodeException, e:
            return None



    def exists(self, path, options):
        """
        Returns boolean
        Parameters:
            path: Stringoptions: int
        @Override


        """
        return self._zkClient.exists(path)


    def getStat(self, path, options):
        """
        Returns Stat
        Parameters:
            path: Stringoptions: int
        @Override


        """
        return self._zkClient.getStat(path)

    def remove(self, *args):
        if len(args)==2 and isinstance(args[0], str):
            return self.remove_path(*args)
        elif len(args)==2 and isinstance(args[0], list):
            return self.remove_paths(*args)
        else:
            raise IllegalArgumentException("Wrong args: %s" % args)

    def remove_path(self, path, options):
        """
        Returns boolean
        Parameters:
            path: Stringoptions: int
        @Override


        """
        try:
            self._zkClient.delete(path)
        except ZookeeperError, e:
            self._zkClient.deleteRecursive(path)

        return True


    def create_paths(self, paths, records, needCreate, pathsCreated, options):
        """
        Returns CreateCallbackHandler[]
        Parameters:
            paths: List<String>records: List<T>needCreate: boolean[]pathsCreated: List<List<String>>options: int


        """
        if (records != None and records.__len__() != paths.__len__()) or needCreate.__len__() != paths.__len__() or (pathsCreated != None and pathsCreated.__len__() != paths.__len__()):
            raise IllegalArgumentException("paths, records, needCreate, and pathsCreated should be of same size")


        # CreateCallbackHandler[]
        cbList = [CreateCallbackHandler() for path in paths]
        # CreateMode
        mode = AccessOption.getMode(options)
        if mode == None: 
            self.LOG.error("Invalid async set mode. options: " + str(options))
            return cbList

        # boolean
        retry = True
        
        while retry:
            retry = False
            for i in range(paths.__len__()):
                if not needCreate[i]:
                    continue
                # String
                path = paths[i]
                # T
                record = ternary(records == None, None, records[i])
                cbList[i] = CreateCallbackHandler()
                self._zkClient.asyncCreate(path, record, mode, cbList[i])

            # List<String>
#            parentPaths = ArrayList<String>(Collections.nCopies(paths.__len__(), None))
            parentPaths = [None for path in paths]
            # boolean
            failOnNoNode = False
            for i in range(paths.__len__()):
                if not needCreate[i]:
                    continue
                # CreateCallbackHandler
                cb = cbList[i]
                cb.waitForSuccess()
                # String
                path = paths[i]
#                if Code.get(cb.getRc()) == Code.NONODE:
                if cb.isNoNodeError():
                    # String
                    parentPath = os.path.abspath(os.path.join(path, os.path.pardir))
#                    parentPath = File(path).getParent()
                    parentPaths[i] = parentPath
                    failOnNoNode = True
                else:
                    needCreate[i] = False
#                    if Code.get(cb.getRc()) == Code.OK and pathsCreated != None:
                    if cb.isSuccessful() and pathsCreated != None:
                        if pathsCreated[i] == None:
                            pathsCreated[i] = []
                        pathsCreated[i].add(path)

            if failOnNoNode:
                # boolean[]
#                needCreateParent = Arrays.copyOf(needCreate, needCreate.length)
                needCreateParent = [x for x in needCreate]
                # CreateCallbackHandler[]
                parentCbList = self.create(parentPaths, None, needCreateParent, pathsCreated, AccessOption.PERSISTENT)
                for parentCb in parentCbList: # CreateCallbackHandler
#                    parentCb = parentCbList[i]
                    if parentCb == None: 
                        continue
                    # Code
                    if parentCb.isSuccessful() or parentCb.isNodeExistError():
#                    rc = Code.get(parentCb.getRc())
#                    if rc == Code.OK or rc == Code.NODEEXISTS:
                        retry = True
                        break

        return cbList


    def createChildren(self, paths, records, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>records: List<T>options: int
        @Override


        """
        # boolean[]
        success = [False for path in paths]
#        success = new boolean[paths.__len__()]
        # CreateMode
        mode = AccessOption.getMode(options)
        if mode == None: 
            self.LOG.error("Invalid async create mode. options: " + str(options))
            return success

        # boolean[]
#        needCreate = new boolean[paths.__len__()]
#        Arrays.fill(needCreate, True)
        needCreate = [True for path in paths]
        # List<List<String>>
#        pathsCreated = ArrayList<List<String>>(Collections.nCopies(paths.__len__(), None))
        pathsCreated = [None for path in paths]
        # long
        startT = time.time()
        try:
            # CreateCallbackHandler[]
            cbList = self.create(paths, records, needCreate, pathsCreated, options)
            for i in range(len(cbList)): # CreateCallbackHandler
                cb = cbList[i]
#                success[i] = (Code.get(cb.getRc()) == Code.OK)
                success[i] = cb.isSuccessful()

            return success
        finally:
                # long
                endT = time.time()
#                if self.LOG.isDebugEnabled():
#                    self.LOG.debug("create_async, size: " + str(paths.__len__())+ ", paths: " + str(paths.__getitem__(0))+ ",... time: " + str((endT - startT) )+ " ms")




    def setChildren(self, paths, records, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>records: List<T>options: int
        @Override


        """
        return self.set(paths, records, None, None, options)


    def set_paths(self, paths, records, pathsCreated, stats, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>records: List<T>pathsCreated: List<List<String>>stats: List<Stat>options: int
f

        """
        if paths == None or paths.__len__() == 0:
#            return new boolean[0]
            return []

        if (records != None and len(records) != len(paths) or (pathsCreated != None and len(pathsCreated) != len(paths))):
            raise IllegalArgumentException("paths, records, and pathsCreated should be of same size")


        # boolean[]
        success = [False for path in paths]
        # CreateMode
        mode = AccessOption.getMode(options)
        if mode == None: 
            self.LOG.error("Invalid async set mode. options: " + str(options))
            return success

        # List<Stat>
#        setStats = [None for path in paths]
        setStats = [HelixZNodeStat() for path in paths]
#        setStats = ArrayList<Stat>(Collections.nCopies(paths.__len__(), None))
        # SetDataCallbackHandler[]
        cbList = [SetDataCallbackHandler for path in paths]
        # CreateCallbackHandler[]
        createCbList = None
        # boolean[]
        needSet = [True for path in paths]
        # long
        startT = time.time()
        try:
            # boolean
            retry = True
            
            while retry:
                retry = False
                for i in range(len(paths)):
                    if not needSet[i]:
                        continue
                    # String
                    path = paths[i]
                    # T
                    record = records[i]
                    cbList[i] = SetDataCallbackHandler()
                    self._zkClient.asyncSetData(path, record, -1, cbList[i])

                # boolean
                failOnNoNode = False
                for i in range(len(cbList)): # SetDataCallbackHandler
                    cb = cbList[i]
                    cb.waitForSuccess()
                    # Code
#                    rc = Code.get(cb.getRc())
#                    switch(rc) {
                    if cb.isSuccessful():
                        setStats[i] = cb.getStat()
                        needSet[i] = False
#                        case NONODE:
                    elif cb.isNoNodeError():
                        failOnNoNode = True
                    else:
                        needSet[i] = False

                if failOnNoNode: 
                    # boolean[]
                    needCreate = copy.copy(needSet)
                    createCbList = self.create(paths, records, needCreate, pathsCreated, options)
                    for i in range(len(createCbList)): # CreateCallbackHandler
                        createCb = createCbList[i]
                        if createCb == None: 
                            continue

                        # Code
#                        rc = Code.get(createCb.getRc())
#                        switch(rc) {
#                            case OK:
                        if createCb.isSuccessful():
                            setStats[i] = ZNode.ZERO_STAT
                            needSet[i] = False
#                        case NODEEXISTS:
                        elif createCb.isNodeExistsError():
                            retry = True
                        else:
                            needSet[i] = False

            for i in range(len(cbList)): # SetDataCallbackHandler
                cb = cbList[i]
                # Code
#                rc = Code.get(cb.getRc())
#                if rc == Code.OK:
                if cb.isSuccessful():
                    success[i] = True
                elif cb.isNoNodeError():
#                    if rc == Code.NONODE:
                        # CreateCallbackHandler
                    createCb = createCbList[i]
#                    if Code.get(createCb.getRc()) == Code.OK:
                    if createCb.isSuccessful():
                        success[i] = True


            if stats != None:
                stats = copy.copy(setStats)
#                stats.clear()
#                stats.addAll(setStats)

            return success
        finally:
            # long
            endT = time.time()
            #if self.LOG.isDebugEnabled():
            self.LOG.debug("setData_async, size: " + str(paths.__len__())+ ", paths: " + str(paths.__getitem__(0))+ ",... time: " + str((endT - startT) )+ " ns")




    def updateChildren(self, paths, updaters, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>updaters: List<DataUpdater<T>>options: int
        @Override


        """
        # List<T>
        updateData = self.update(paths, updaters, None, None, options)
        # boolean[]
        success = [False for path in paths]
        for i in range(len(paths)): # T
            data = updateData[i]
            success[i] = (data != None)

        return success


    def update_paths(self, paths, updaters, pathsCreated, stats, options):
        """
        Returns List<T>
        Parameters:
            paths: List<String>updaters: List<DataUpdater<T>>pathsCreated: List<List<String>>stats: List<Stat>options: int


        """
        if paths == None or len(paths) == 0:
            self.LOG.error("paths is null or empty")
#            return Collections.emptyList()
            return []

        if len(updaters) != len(paths) or (pathsCreated != None and len(pathsCreated) != len(paths)):
            raise IllegalArgumentException("paths, updaters, and pathsCreated should be of same size")


        # List<Stat>
        setStats = [ZnodeStat for path in paths]
        # List<T>
        updateData = [None for path in paths]
        # CreateMode
        mode = AccessOption.getMode(options)
        if mode == None: 
            self.LOG.error("Invalid update mode. options: " + str(options))
            return updateData

        # SetDataCallbackHandler[]
        cbList =[SetDataCallbackHandler() for path in paths]
        # CreateCallbackHandler[]
        createCbList = None
        # boolean[]
        needUpdate = [True for path in paths]
        # long
        startT = time.time()
        try:
            # boolean
            retry = True
            
            while retry:
                retry = False
                # boolean[]
                needCreate = [False for path in paths]
                # boolean
                failOnNoNode = False
                # List<Stat>
                curStats = []
                # List<T>
                curDataList = self.get(paths, curStats, copy.copy(needUpdate))
                # List<T>
                newDataList = []
#                newDataList = ArrayList<T>()
                for i in range(len(paths)):
                    if not needUpdate[i]:
                        newDataList.append(None)
                        continue

                    # String
                    path = paths[i]
                    # DataUpdater<T>
                    updater = updaters[i]
                    # T
                    newData = updater.update(curDataList[i])
                    newDataList.append(newData)
                    # Stat
                    curStat = curStats[i]
                    if curStat == None: 
                        failOnNoNode = True
                        needCreate[i] = True
                    else:
                        cbList[i] = SetDataCallbackHandler()
                        self._zkClient.asyncSetData(path, newData, curStat.getVersion(), cbList[i])


                # boolean
                failOnBadVersion = False
                for i in range(len(paths)): # SetDataCallbackHandler
                    cb = cbList[i]
                    if cb == None: 
                        continue
                    cb.waitForSuccess()
#                    switch(Code.get(cb.getRc())) {
#                        case OK:
                    if cb.isSuccessful():
                            updateData.__setitem__(i, newDataList[i])
                            setStats.__setitem__(i, cb.getStat())
                            needUpdate[i] = False
#                        case NONODE:
                    elif cb.isNoNodeError():
                            failOnNoNode = True
                            needCreate[i] = True
#                        case BADVERSION:
                    elif cb.isBadVersionError():
                            failOnBadVersion = True
                    else:
                            needUpdate[i] = False
                            break

                if failOnNoNode: 
                    createCbList = self.create(paths, newDataList, needCreate, pathsCreated, options)
                    for i in range(len(paths)): # CreateCallbackHandler
                        createCb = createCbList[i]
                        if createCb == None: 
                            continue

#                        switch(Code.get(createCb.getRc())) {
#                            case OK:
                        if createCb.isSuccessful():
                                needUpdate[i] = False
                                updateData.__setitem__(i, newDataList[i])
                                setStats.__setitem__(i, ZNode.ZERO_STAT)
#                            case NODEEXISTS:
                        elif createCb.isNodeExistsError:
                                retry = True
                        else:
                                needUpdate[i] = False

                if failOnBadVersion: 
                    retry = True

            if stats != None:
                stats = copy.copy(setStats)
#                stats.clear()
#                stats.addAll(setStats)

            return updateData
        finally:
                # long
                endT = time.time()
#                if self.LOG.isDebugEnabled():
                self.LOG.debug("setData_async, size: " + str(paths.__len__())+ ", paths: " + str(paths.__getitem__(0))+ ",... time: " + str((endT - startT) )+ " ns")




    def exists(self, paths, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>options: int
        @Override


        """
        # Stat[]
        stats = self.getStats(paths, options)
        # boolean[]
        exists = [stats[i] != None for i in range(len(paths))]
#        for i in range(len(paths)):
#            exists[i] = (stats[i] != None)
        return exists


    def getStats(self, paths, options):
        """
        Returns Stat[]
        Parameters:
            paths: List<String>options: int
        @Override


        """
        if paths == None or paths.__len__() == 0:
            self.LOG.error("paths is null or empty")
#            return new Stat[0]
            return []
        # Stat[]
        stats = [ZnodeStat() for path in paths]
        # long
        startT = time.time()
        try:
            # ExistsCallbackHandler[]
            cbList = [ExistsCallbackHandler() for path in paths.__len__]
            for i in range(len(paths)): # String
                path = paths[i]
                cbList[i] = ExistsCallbackHandler()
                self._zkClient.asyncExists(path, cbList[i])

            for i in range(len(cbList)): # ExistsCallbackHandler
                cb = cbList[i]
                cb.waitForSuccess()
                stats[i] = cb._stat

            return stats
        finally:
                # long
                endT = time.time()
#                if self.LOG.isDebugEnabled():
                self.LOG.debug("exists_async, size: " + str(paths.__len__())+ ", paths: " + str(paths.__getitem__(0))+ ",... time: " + str((endT - startT) )+ " ns")




    def remove_paths(self, paths, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>options: int
        @Override


        """
        if not paths:
            return []

        # boolean[]
        success = [False for path in paths]
        # DeleteCallbackHandler[]
        cbList = [DeleteCallbackHandler() for path in paths]
        # long
        startT = time.time()
        try:
            for i in range(len(paths)): # String
                path = paths[i]
                cbList[i] = DeleteCallbackHandler()
                self._zkClient.asyncDelete(path, cbList[i])

            for i in range(len(cbList)): # DeleteCallbackHandler
                cb = cbList[i]
                cb.waitForSuccess()
                success[i] = cb._stat

            return success
        finally:
                # long
                endT = time.time()
#                if self.LOG.isDebugEnabled():
                self.LOG.debug("delete_async, size: " + str(paths.__len__())+ ", paths: " + str(paths.__getitem__(0))+ ",... time: " + str((endT - startT) )+ " ns")




    def subscribeDataChanges(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: IZkDataListener
        @Override


        """
        self._zkClient.subscribeDataChanges(path, listener)


    def unsubscribeDataChanges(self, path, dataListener):
        """
        Returns void
        Parameters:
            path: StringdataListener: IZkDataListener
        @Override


        """
        self._zkClient.unsubscribeDataChanges(path, dataListener)


    def subscribeChildChanges(self, path, listener):
        """
        Returns List<String>
        Parameters:
            path: Stringlistener: IZkChildListener
        @Override


        """
        return self._zkClient.subscribeChildChanges(path, listener)


    def unsubscribeChildChanges(self, path, childListener):
        """
        Returns void
        Parameters:
            path: StringchildListener: IZkChildListener
        @Override


        """
        self._zkClient.unsubscribeChildChanges(path, childListener)


#    def main(args):
#        """
#        Returns void
#        Parameters:
#            args: String[]
#        Java modifiers:
#             static
#
#        """
#        # ZkClient
#        zkclient = ZkClient("localhost:2191")
#        zkclient.setZkSerializer(ZNRecordSerializer())
#        # ZkBaseDataAccessor<ZNRecord>
#        accessor = ZkBaseDataAccessor<ZNRecord>(zkclient)
#        # List<String>
#        createPaths = Arrays.asList("/test/child1/child1", "/test/child2/child2")
#        # List<ZNRecord>
#        createRecords = Arrays.asList(ZNRecord("child1"), ZNRecord("child2"))
#        # boolean[]
#        needCreate = new boolean[createPaths.size()]
#        Arrays.fill(needCreate, True)
#        # List<List<String>>
#        pathsCreated = ArrayList<List<String>>(Collections.nCopies(createPaths.size(), None))
#        accessor.create(createPaths, createRecords, needCreate, pathsCreated, AccessOption.PERSISTENT)
#        System.out.println("pathsCreated: " + pathsCreated)
#        # List<String>
#        setPaths = Arrays.asList("/test/setChild1/setChild1", "/test/setChild2/setChild2")
#        # List<ZNRecord>
#        setRecords = Arrays.asList(ZNRecord("setChild1"), ZNRecord("setChild2"))
#        pathsCreated = ArrayList<List<String>>(Collections.nCopies(setPaths.size(), None))
#        # boolean[]
#        success = accessor.set(setPaths, setRecords, pathsCreated, None, AccessOption.PERSISTENT)
#        System.out.println("pathsCreated: " + pathsCreated)
#        System.out.println("setSuccess: " + Arrays.toString(success))
#        # List<String>
#        updatePaths = Arrays.asList("/test/updateChild1/updateChild1", "/test/setChild2/setChild2")
#
#        class TestUpdater(DataUpdater<ZNRecord>):
#
#
#
#            """
#
#            Parameters:
#                ZNRecord newData
#            """
#            def __init__(self, newData):
#                self._newData = newData
#
#
#            def update(self, currentData):
#                """
#                Returns ZNRecord
#                Parameters:
#                    currentData: ZNRecord
#                @Override
#
#
#                """
#                return _newData
#
#
#        # List<DataUpdater<ZNRecord>>
#        updaters = Arrays.asList((DataUpdater<ZNRecord>) TestUpdater(ZNRecord("updateChild1")), (DataUpdater<ZNRecord>) TestUpdater(ZNRecord("updateChild2")))
#        pathsCreated = ArrayList<List<String>>(Collections.nCopies(updatePaths.size(), None))
#        # List<ZNRecord>
#        updateRecords = accessor.update(updatePaths, updaters, pathsCreated, None, AccessOption.PERSISTENT)
#        for (# int
#        i = 0; i < updatePaths.size(); i++) success[i] = updateRecords.get(i) != None
#
#        System.out.println("pathsCreated: " + pathsCreated)
#        System.out.println("updateSuccess: " + str(success))
#        System.out.println("CLOSING")
#        zkclient.close()


    def reset(self):
        """
        Returns void
        @Override


        """
        pass


