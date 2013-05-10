# package org.apache.helix.agent
#from org.apache.helix.agent import *
#from java.io import File
#from java.util import Random
#from org.apache.helix.agent.SystemUtil import ProcessStateCode
#from org.apache.log4j import Logger
import random
import threading
import time
from org.apache.helix.agent.SystemUtil import SystemUtil
from org.apache.helix.agent.SystemUtil import ProcessStateCode
from org.apache.helix.util.logger import get_logger


class ProcessMonitorThread(threading.Thread):

    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """
    Java modifiers:
         private final static
    Type:
        int
    """
    MONITOR_PERIOD_BASE = 1000

    """

    Parameters:
        String pid
    """
    def __init__(self, pid):
        self._pid = pid


    def run(self):
        """
        Returns void
        @Override


        """
        try:
            # ProcessStateCode
            processState = SystemUtil.getProcessState(self._pid)
            while processState != None:
                if processState == ProcessStateCode.Z: 
                    self.logger.error("process: " + self._pid + " is in zombie state")
                    break

                time.sleep((random.randint(0,ProcessMonitorThread.MONITOR_PERIOD_BASE) + ProcessMonitorThread.MONITOR_PERIOD_BASE)/1000.0)
                processState = SystemUtil.getProcessState(self._pid)

        except Exception, e:
            ProcessMonitorThread.logger.error("fail to monitor process: " + self._pid, e)




