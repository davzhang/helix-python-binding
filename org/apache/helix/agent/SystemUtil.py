# package org.apache.helix.agent
#from org.apache.helix.agent import *
#from java.io import BufferedReader
#from java.io import File
#from java.io import FileReader
#from java.io import IOException
#from org.apache.helix import ExternalCommand
#from org.apache.log4j import Logger
import platform
import os
import re
from org.apache.helix.util.logger import get_logger

class ProcessStateCodeProperty(object):
    def __init__(self, description):
        self._description = description

    def getDescription(self):
        return self._description


class ProcessStateCode(object):
    enumDict = { "D":"Uninterruptible sleep (usually IO)",
                 "R":"Running or runnable (on run queue)",
                 "S":"Interruptible sleep (waiting for an event to complete)",
                 "T":"Stopped, either by a job control signal or because it is being traced.",
                 "W":"paging (not valid since the 2.6.xx kernel)",
                 "X":"dead (should never be seen)",
                 "Z":"Defunct (\"zombie\") process, terminated but not reaped by its parent."}
    @staticmethod
    def valueOf(self, name):
        return self.__getattribute__(name)

# dynamically add the attributes
for key in ProcessStateCode.enumDict:
    processStateCodeProperty = ProcessStateCodeProperty(ProcessStateCode.enumDict[key])
    setattr(ProcessStateCode, key, processStateCodeProperty)


class SystemUtil(object):

    OS_NAME = platform.system()

    logger = get_logger(__name__)

    @staticmethod
    def getProcessState(processId):
        """
        Returns ProcessStateCode
        Parameters:
            processId: String
        Java modifiers:
             static

        Throws: 
            Exception
        """
        if (SystemUtil.OS_NAME == "Mac OS X") or (SystemUtil.OS_NAME == "Linux"):
            # ExternalCommand
            # cmd = ExternalCommand.start("ps", processId)
            # cmd.waitFor()
            output = os.popen("ps %s" % processId).read()
            # String
            # lines = cmd.getStringOutput().split("\n")
            lines = output.split("\n")
            if len(lines) != 2:
                SystemUtil.logger.info("process: " + processId + " not exist")
                return None

            attributes = re.split("\s+", lines[0].strip())
            values = re.split("\s+", lines[1].strip())
            # Character
            processStateCodeChar = None
            for i in range(len(attributes)):
                attribute = attributes[i]
                if ("STAT" == attribute) or ("S" == attribute): 
                    processStateCodeChar = values[i][0]
                    break
            return ProcessStateCode.valueOf(processStateCodeChar)
        else:
            raise ("Not supported OS: " + SystemUtil.OS_NAME)

    @staticmethod
    def getPidFromFile(file):
        try:
            with open(file) as pidFile:
              line = pidFile.readline()
              return line
        except IOError, e:
            SystemUtil.logger.error("fail to read pid from pidFile: " + file, e)
        finally:
            return None




