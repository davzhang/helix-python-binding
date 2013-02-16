# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from org.I0Itec.zkclient import IZkStateListener
#from org.I0Itec.zkclient import ZkConnection
#from org.apache.log4j import Logger
#from org.apache.zookeeper.Watcher.Event import KeeperState
from kazoo.protocol.states import KazooState

from org.apache.helix.util.logger import get_logger
#from kazoo.protocol.states import KeeperState

#class ZkStateChangeListener(IZkStateListener):
from org.apache.helix.HelixConstants import KeeperState


class ZkStateChangeListener():

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """

    Parameters:
        ZKHelixManager zkHelixManager
    """
    def __init__(self, zkHelixManager):
        self._zkHelixManager = zkHelixManager
        self._isConnected = False
        self._hasSessionExpired = False


    def handleNewSession(self):
        """
        Returns void
        @Override


        """
        self._isConnected = True
        self._hasSessionExpired = False
        self._zkHelixManager.handleNewSession()



    def handleStateChanged(self, kazooState):
        ''' keeperState can be obtained from zkConnection.client_state
        '''
        if kazooState == KazooState.CONNECTED:
            zkConnection = self._zkHelixManager._zkClient.getConnection()
            self.logger.info("KeeperState: " + kazooState + ", zookeeper:" + str(zkConnection.hosts))
            _isConnected = True
        elif kazooState == KazooState.SUSPENDED:   # disconnect
            self.logger.info("KeeperState:" + kazooState + ", disconnectedSessionId: " + self._zkHelixManager._sessionId + ", instance: " + self._zkHelixManager.getInstanceName() + ", type: " + self._zkHelixManager.getInstanceType())
            _isConnected = False
        elif kazooState == KazooState.LOST:
        #            case Expired:
            self.logger.info("KeeperState:" + kazooState + ", expiredSessionId: " + self._zkHelixManager._sessionId + ", instance: " + self._zkHelixManager.getInstanceName() + ", type: " + self._zkHelixManager.getInstanceType())
            _isConnected = False
            _hasSessionExpired = True
#
#
#
#    def handleStateChanged(self, keeperState):
#        """
#        Returns void
#        Parameters:
#            keeperState: KeeperState
#        @Override
#
#
#        Throws:
#            Exception
#        """
#        if keeperState == KeeperState.SyncConnected:
##            case SyncConnected:
#                # ZkConnection
#            zkConnection = self._zkHelixManager._zkClient.getConnection()
#            self.logger.info("KeeperState: " + keeperState + ", zookeeper:" + zkConnection.getZookeeper())
#            _isConnected = True
#        elif keeperState == KeeperState.Disconnected:
##            case Disconnected:
#                self.logger.info("KeeperState:" + keeperState + ", disconnectedSessionId: " + self._zkHelixManager._sessionId + ", instance: " + self._zkHelixManager.getInstanceName() + ", type: " + self._zkHelixManager.getInstanceType())
#                _isConnected = False
#        elif keeperState == KeeperState.Expired:
##            case Expired:
#                self.logger.info("KeeperState:" + keeperState + ", expiredSessionId: " + self._zkHelixManager._sessionId + ", instance: " + self._zkHelixManager.getInstanceName() + ", type: " + self._zkHelixManager.getInstanceType())
#                _isConnected = False
#                _hasSessionExpired = True

    def isConnected(self):
        """
        Returns boolean


        """
        return self._isConnected


    def disconnect(self):
        """
        Returns void


        """
        _isConnected = False


    def hasSessionExpired(self):
        """
        Returns boolean


        """
        return self._hasSessionExpired



