# package org.apache.helix.agent
#from org.apache.helix.agent import *
#from java.io import File
#from java.util import Arrays
#from java.util import List
#from java.util import Map
#from java.util.regex import Matcher
#from java.util.regex import Pattern
#from org.apache.helix import ExternalCommand
import time
from org.apache.helix import HelixManager
from org.apache.helix import NotificationContext
#from org.apache.helix.model import HelixConfigScope
from org.apache.helix.ConfigScope import ConfigScopeProperty
from org.apache.helix.ConfigScopeBuilder import ConfigScopeBuilder
from org.apache.helix.agent.CommandAttribute import CommandAttribute
from org.apache.helix.agent.SystemUtil import SystemUtil
from org.apache.helix.agent.ProcessMonitorThread import ProcessMonitorThread
from org.apache.helix.model.Message import Message
#from org.apache.helix.model.HelixConfigScope import ConfigScopeProperty
# from org.apache.helix.model.builder import HelixConfigScopeBuilder
from org.apache.helix.participant.statemachine.StateModel import StateModel
# from org.apache.helix.participant.statemachine.StateModelInfo import StateModelInfo
# from org.apache.helix.participant.statemachine.Transition import Transition
from org.apache.helix.util.logger import get_logger
import re
from org.apache.helix.util.misc import ExternalCommand


class AgentStateModel(StateModel):

    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    logger = get_logger(__name__)


    """
    Java modifiers:
         private static
    Type:
        Pattern
    """
    pattern = re.compile("({.+?})")

    @staticmethod
    def buildKey(fromState, toState, attribute):
        """
        Returns String
        Parameters:
            fromState: StringtoState: Stringattribute: CommandAttribute
        Java modifiers:
             private static

        """
        return fromState + "-" + toState + "." + attribute.getName()

    @staticmethod
    def instantiateByMessage(template, message):
        """
        Returns String
        Parameters:
            string: Stringmessage: Message
        Java modifiers:
             private static

        """
        # Matcher
        matches = AgentStateModel.pattern.findall(template)
        # String
        for var in matches:
            varName = var.lstrip("{").rstrip("}")
            result = result.replace(var, message.getAttribute(Message.Attributes.__getattribute__(varName)))

        return result


    def genericStateTransitionHandler(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        Throws: 
            Exception
        """
        cmd = message.getRecord().getSimpleField(CommandAttribute.COMMAND.getName())
        workingDir = message.getRecord().getSimpleField(CommandAttribute.WORKING_DIR.getName())
        timeout = message.getRecord().getSimpleField(CommandAttribute.TIMEOUT.getName())
        pidFile = message.getRecord().getSimpleField(CommandAttribute.PID_FILE.getName())

        # HelixManager
        manager = context.getManager()
        clusterName = manager.getClusterName()
        fromState = message.getFromState()
        toState = message.getToState()
        self.logger.info("AgentStateModel.onBecome " + fromState + " From " + toState + " for " + message.getResourceName())

        cmdKey = self.buildKey(fromState, toState, CommandAttribute.COMMAND)
        workingDirKey = self.buildKey(fromState, toState, CommandAttribute.WORKING_DIR)
        timeoutKey = self.buildKey(fromState, toState, CommandAttribute.TIMEOUT)
        pidFileKey = self.buildKey(fromState, toState, CommandAttribute.PID_FILE)
        # List<String>
        cmdConfigKeys = [cmdKey, workingDirKey, timeoutKey, pidFileKey]
        if cmd is None:
            # HelixConfigScope
            # resourceScope = HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName).forResource(message.getResourceName()).build()
            resourceScope = ConfigScopeBuilder().forCluster(clusterName).forResource(message.getResourceName()).build()
            # Map<String, String>
            cmdKeyValueMap = manager.getConfigAccessor().get(resourceScope, cmdConfigKeys)
            if cmdKeyValueMap is not None:
                cmd = cmdKeyValueMap.get(cmdKey)
                workingDir = cmdKeyValueMap.get(workingDirKey)
                timeout = cmdKeyValueMap.get(timeoutKey)
                pidFile = cmdKeyValueMap.get(pidFileKey)


        if cmd is None:
            # HelixConfigScope
            # clusterScope = HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build()
            clusterScope = ConfigScopeBuilder().forCluster(clusterName).build()
            # Map<String, String>
            cmdKeyValueMap = manager.getConfigAccessor().get(clusterScope, cmdConfigKeys)
            if cmdKeyValueMap is not None:
                cmd = cmdKeyValueMap.get(cmdKey)
                workingDir = cmdKeyValueMap.get(workingDirKey)
                timeout = cmdKeyValueMap.get(timeoutKey)
                pidFile = cmdKeyValueMap.get(pidFileKey)


        if cmd is None:
            raise Exception("Unable to find command for transition from:" + message.getFromState() + " to:" + message.getToState())

        self.logger.info("Executing command: " + cmd + ", using workingDir: " + str(workingDir) + ", timeout: " + str(timeout) + ", on " + str(manager.getInstanceName()))
        if cmd == CommandAttribute.NOP.getName():
            return

        # String
        cmdSplits = re.split("\s+", cmd.strip())
        # String
        cmdValue = cmdSplits[0]
        # String
        args = cmdSplits[1:]
        # long
        timeoutValue = 0
        if timeout is not None:
            try:
                timeoutValue = long(timeout)
            except: pass   # ok to use 0


        # ExternalCommand
        externalCmd = ExternalCommand.executeWithTimeout(workingDir, cmdValue, timeoutValue, args)
        # int
        exitValue = externalCmd.exitValue()
        self.logger.info("Executed command: " + cmd + ", exitValue: " + str(exitValue))
        if exitValue != 0: 
            raise Exception("fail to execute command: " + cmd + ", exitValue: " + str(exitValue) + ", error: " + externalCmd.getStringError())

        if pidFile is not None:
            return

        # String
        pidFileValue = self.instantiateByMessage(pidFile, message)
        # String
        pid = SystemUtil.getPidFromFile(pidFileValue)
        if pid is not None:
            ProcessMonitorThread(pid).start()

    def onBecomeSlaveFromOffline(self, message, context):
        # self.logger.info("AgentStateModel.onBecomeSlaveFromOffline() for " + self.stateUnitKey)
        self.genericStateTransitionHandler(message, context)

    def onBecomeSlaveFromMaster(self, message, context):
        # self.logger.info("AgentStateModel.onBecomeSlaveFromMaster() for " + self.stateUnitKey)
        self.genericStateTransitionHandler(message, context)

    def onBecomeMasterFromSlave(self, message, context):
        # self.logger.info("AgentStateModel.onBecomeMasterFromSlave() for " + self.stateUnitKey)
        self.genericStateTransitionHandler(message, context)

    def onBecomeOfflineFromSlave(self, message, context):
        # self.logger.info("AgentStateModel.onBecomeOfflineFromSlave() for " + self.stateUnitKey)
        self.genericStateTransitionHandler(message, context)

    def onBecomeDroppedFromOffline(self, message, context):
        # self.logger.info("AgentStateModel.onBecomeDroppedFromOffline() for " + self.stateUnitKey)
        self.genericStateTransitionHandler(message, context)




