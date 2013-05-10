import threading
import unittest
import os
import sys
from org.apache.helix.ConfigScopeBuilder import ConfigScopeBuilder
from org.apache.helix.ConfigAccessor import ConfigAccessor
from org.apache.helix.ConfigScope import ConfigScopeProperty
from org.apache.helix.agent.CommandAttribute import CommandAttribute
from org.apache.helix.agent.CommandConfig import CommandConfig
from org.apache.helix.agent import HelixAgentMain   # import the file
from org.apache.helix.agent.SystemUtil import SystemUtil
from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.test.TestHelper import TestHelper


class TestHelixAgent(unittest.TestCase):

    def test_agent(self):

        zkAddr = "localhost:2199"
        numberofNodes = 1
        # clusterName = "%s_%s" % (self.__name__, __name__)  # class name, function name
        # clusterName = "%s_%s" % (self.__class__name__, sys._getframe(1).f_code.co_name )  # class name, function name
        clusterName = "testCluster"  # class name, function name

        TestHelper.setupCluster(clusterName,
                                zkAddr,
                            12918, # participant port
                            "localhost", # participant name prefix
                            "TestDB", # resource name prefix
                            1, # resources
                            1, # partitions per resource
                            numberofNodes, # number of nodes
                            1, # replicas
                            "MasterSlave",
                            True) # do rebalance

        # setup the config
#        import pdb; pdb.set_trace()
        client = ZkClient(zkAddr)
        scope = ConfigScopeBuilder().forCluster(clusterName).build()
        configAccessor = ConfigAccessor(client)
        workingDir = "/tmp"
        pidFile = "/tmp/foo_{PARTITION_NAME}_pid.txt"
        pidFileFirstPartition = "/tmp/foo_TestDB_0_pid.txt"
        if os.path.exists(pidFileFirstPartition):
            os.remove(pidFileFirstPartition)

        builder = CommandConfig.Builder()
        cmdConfig = builder.setTransition("SLAVE", "MASTER")\
                        .setCommand("/tmp/t2.sh")\
                        .setCommandWorkingDir(workingDir)\
                        .setCommandTimeout("0")\
                        .setPidFile(pidFile).build()

        # TODO: need to fix the set
        configAccessor.set(scope, cmdConfig.toKeyValueMap())

        for (fromState, toState) in (("OFFLINE", "SLAVE"), ("MASTER", "SLAVE"), ("SLAVE", "OFFLINE"), ("OFFLINE", "DROP")):
            builder = CommandConfig.Builder()
            cmdConfig = builder.setTransition("OFFLINE", "SLAVE") \
                            .setCommand(CommandAttribute.NOP.getName())
            configAccessor.set(scope, cmdConfig.toKeyValueMap())

        TestHelper.startController(clusterName,
                                zkAddr)

        # start Helix-agent
        class AgentThread(threading.Thread):
            def __init__(self, instanceName):
                threading.Thread.__init__(self)
                self._instanceName = instanceName
            def run(self):
                HelixAgentMain.main(["--zkSvr", zkAddr, "--cluster", clusterName,
                "--instanceName", self._instanceName, "--stateModel", "MasterSlave"])
        agentThreads=[]
        for i in range(numberofNodes):
            instanceName = "localhost_" + str(12918 + i)
            agentThread = AgentThread(instanceName)
            agentThreads.append(agentThread)
            agentThread.start()

        # self.assertTrue(True)
        # read pid
        readPid = SystemUtil.getPidFromFile(pidFileFirstPartition)
        self.assertTrue(readPid is not None, "readPid should NOT be null")

    # def test_agent_1(self):
    #     self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()
