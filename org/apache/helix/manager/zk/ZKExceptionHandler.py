# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from org.I0Itec.zkclient.exception import ZkInterruptedException
#from org.apache.log4j import Logger

from org.apache.helix.util.logger import get_logger
from kazoo.exceptions import SystemZookeeperError
import threading

class ZKExceptionHandler:

    """
    Java modifiers:
         private static
    Type:
        ZKExceptionHandler
    """
#    instance = ZKExceptionHandler()
    instance=None

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """
    Java modifiers:
         private
    """
    def __init__(self):
        if ZKExceptionHandler.instance:
            ZKExceptionHandler.instance = ZKExceptionHandler()


    def handle(self, e):
        """
        Returns void
        Parameters:
            e: Exception


        """
#        self.logger.error(threading.current_thread + ". isThreadInterruped: " + str(Thread.currentThread().isInterrupted()))
#        if type(e) == ZkInterruptedException:
        if type(e) == SystemZookeeperError:
            self.logger.error("zk error."+ str(e))
#            self.logger.error("zk connection is interrupted."+ str(e))
        else:
            self.logger.error(e)


    @staticmethod
    def getInstance():
        """
        Returns ZKExceptionHandler
        Java modifiers:
             static

        """
        if not ZKExceptionHandler.instance:
            ZKExceptionHandler.instance = ZKExceptionHandler()
        return ZKExceptionHandler.instance



