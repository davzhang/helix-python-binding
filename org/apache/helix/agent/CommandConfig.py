# package org.apache.helix.agent
#from org.apache.helix.agent import *
#from java.util import Map
#from java.util import TreeMap
from org.apache.helix.agent.CommandAttribute import CommandAttribute


class CommandConfig:

    def __init__(self, fromState, toState, command, workingDir, timeout, pidFile):
        self._fromState = fromState
        self._toState = toState
        self._command = command
        self._workingDir = workingDir
        self._timeout = timeout
        self._pidFile = pidFile


    def buildKey(self, fromState, toState, attribute):
        return fromState + "-" + toState + "." + attribute.getName()


    def toKeyValueMap(self):
        """
        Returns Map<String, String>
        """
        # Map<String, String>
        map = {}
        map[self.buildKey(self._fromState, self._toState, CommandAttribute.COMMAND)] = self._command
        if not (self._command == CommandAttribute.NOP.getName()):
            map[self.buildKey(self._fromState, self._toState, CommandAttribute.WORKING_DIR)] = self._workingDir
            map[self.buildKey(self._fromState, self._toState, CommandAttribute.TIMEOUT)] = self._timeout
            map[self.buildKey(self._fromState, self._toState, CommandAttribute.PID_FILE)] = self._pidFile

        return map


    class Builder:

        def setTransition(self, fromState, toState):
            """
            Returns Builder
            Parameters:
                fromState: StringtoState: String


            """
            self._fromState = fromState
            self._toState = toState
            return self


        def setCommand(self, command):
            """
            Returns Builder
            Parameters:
                command: String


            """
            self._command = command
            return self


        def setCommandWorkingDir(self, workingDir):
            """
            Returns Builder
            Parameters:
                workingDir: String


            """
            self._workingDir = workingDir
            return self


        def setCommandTimeout(self, timeout):
            """
            Returns Builder
            Parameters:
                timeout: String


            """
            self._timeout = timeout
            return self


        def setPidFile(self, pidFile):
            """
            Returns Builder
            Parameters:
                pidFile: String


            """
            self._pidFile = pidFile
            return self


        def build(self):
            """
            Returns CommandConfig


            """
            return CommandConfig(self._fromState, self._toState, self._command, self._workingDir, self._timeout, self._pidFile)




