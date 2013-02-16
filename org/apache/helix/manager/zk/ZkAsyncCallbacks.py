# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util.concurrent.atomic import AtomicBoolean
#from org.apache.log4j import Logger
#from org.apache.zookeeper.AsyncCallback import DataCallback
#from org.apache.zookeeper.AsyncCallback import StatCallback
#from org.apache.zookeeper.AsyncCallback import StringCallback
#from org.apache.zookeeper.AsyncCallback import VoidCallback
#from org.apache.zookeeper.KeeperException import Code
#from org.apache.zookeeper.data import Stat
import threading
from kazoo.exceptions import NoNodeError, NodeExistsError, BadVersionError

from org.apache.helix.util.logger import get_logger
from kazoo.interfaces import IAsyncResult

LOG = get_logger(__name__)

#class ZkAsyncCallbacks:
#
#    """
#    Java modifiers:
#         private static
#    Type:
#        Logger
#    """


class DefaultCallback(object):

    def __init__(self):
        self._asyncResult = None
        self._cv = threading.Condition()
        self._success = False
        self._finished = False   # can also check _asyncResult
        self._stat = None   # common

    def callback(self, asyncResult):
        """
        Returns void
        Parameters:
            asyncResult: IAsyncResult


        """
        self._asyncResult = asyncResult
        if not asyncResult.successful():
            LOG.warn(str(self) + ",rc:" + str(asyncResult) + "failed")

        self._cv.acquire()
        try:
            self._success = asyncResult.successful and not asyncResult.exception
            self._finished=True
            if self._success: self.handle()
            else: LOG.ERROR("Async call failed or has exception: %s" % asyncResult.exception)
            self._cv.notify()
        finally:
            self._cv.release()


    def waitForSuccess(self):
        """
        Returns boolean, should be call waitForFinish


        """
        self._cv.acquire()
        try:
#           self._lock.wait(10)
           if self._finished: return self._success
           self._cv.wait()
#           self._cv.wait(10)
           if not self._finished:
               raise BaseException("Got notified but is not finished")
        finally:
            self._cv.release()

        return self._success

    def isSuccessful(self):
        return self._success

    def isNoNodeError(self):
        return self._asyncResult.exception and self._asyncResult.exception == NoNodeError

    def isNodeExistsError(self):
        return self._asyncResult.exception and self._asyncResult.exception == NodeExistsError

    def isBadVersionError(self):
        return self._asyncResult.exception and self._asyncResult.exception == BadVersionError

    def getStat(self):
        return self._stat

    def handle(self):
        ''' get the data
        '''

class GetDataCallbackHandler(DefaultCallback):

    def __init__(self):
        super(GetDataCallbackHandler,self).__init__()
        self._data = None
#        self._stat = None

    def handle(self):
        """
        Returns void
        @Override

        """
        # the value returned is a tuple (data, ZNodeStat)
        self._data, self._stat = self._asyncResult.get_nowait()


SetDataCallbackHandler=DefaultCallback
DefaultCallbackHandler=DefaultCallback
ExistsCallbackHandler=DefaultCallback
CreateCallbackHandler=DefaultCallback
DeleteCallbackHandler=DefaultCallback
#
#
#    def handle(self):
#        """
#        Returns void
#        @Override
#
#
#        """
#
#
#    def processResult(self, rc, path, ctx, stat):
#        """
#        Returns void
#        Parameters:
#            rc: intpath: Stringctx: Objectstat: Stat
#        @Override
#
#
#        """
#        if rc == 0:
#            _stat = stat
#
#        callback(rc, path, ctx)
#
#
#    def getStat(self):
#        """
#        Returns Stat
#
#
#        """
#        return _stat
#
#
#
#class ExistsCallbackHandler(DefaultCallback, StatCallback):
#
#
#
#    def handle(self):
#        """
#        Returns void
#        @Override
#
#
#        """
#
#
#    def processResult(self, rc, path, ctx, stat):
#        """
#        Returns void
#        Parameters:
#            rc: intpath: Stringctx: Objectstat: Stat
#        @Override
#
#
#        """
#        if rc == 0:
#            _stat = stat
#
#        callback(rc, path, ctx)
#
#
#
#class CreateCallbackHandler(DefaultCallback, StringCallback):
#
#    def processResult(self, rc, path, ctx, name):
#        """
#        Returns void
#        Parameters:
#            rc: intpath: Stringctx: Objectname: String
#        @Override
#
#
#        """
#        callback(rc, path, ctx)
#
#
#    def handle(self):
#        """
#        Returns void
#        @Override
#
#
#        """
#
#
#
#class DeleteCallbackHandler(DefaultCallback, VoidCallback):
#
#    def processResult(self, rc, path, ctx):
#        """
#        Returns void
#        Parameters:
#            rc: intpath: Stringctx: Object
#        @Override
#
#
#        """
#        callback(rc, path, ctx)
#
#
#    def handle(self):
#        """
#        Returns void
#        @Override
#
#
#        """
#
#
#
#
#
