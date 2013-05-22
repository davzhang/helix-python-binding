import os
import socket


class TestHelper(object):
    CurrentDir = None
    ZK_PORT=2199

    # def __init__(self, helixBinDir=None):
    #   if helixBinDir:
    #       self.HELIX_BIN_DIR = helixBinDir

    @staticmethod
    def setupCluster(clusterName,
                     zkAddr,
                     startPort,
                     participantNamePrefix,
                     resourceNamePrefix,
                     resourceNb,
                     partitionNb,
                     nodesNb,
                     replica,
                     stateModelDef,
                     doRebalance):
        if TestHelper.isOpen(TestHelper.ZK_PORT):
            print "port %s is open. Assume the setup is already done" % TestHelper.ZK_PORT
            return
        if "HELIX_BIN_DIR" in os.environ:
            HELIX_BIN_DIR= os.environ["HELIX_BIN_DIR"]
        else:
            HELIX_BIN_DIR="%s/project/incubator-helix/helix-core/target/helix-core-pkg/bin" % os.environ["HOME"]
        if not os.path.exists(HELIX_BIN_DIR):
            raise Exception("HELIX_BIN_DIR is not defined")
        cwd = os.getcwd()
        os.chdir(HELIX_BIN_DIR)

        # start zk
        os.system("./start-standalone-zookeeper.sh %s &" % zkAddr.split(":")[-1])
        # create the cluster
        os.system("./helix-admin.sh --zkSvr %s --addCluster %s" % (zkAddr, clusterName))

        # create node
        for i in range(nodesNb):
            port = startPort + i
            os.system("./helix-admin.sh --zkSvr %s --addNode %s %s" % (zkAddr, clusterName, "%s_%s" % (participantNamePrefix, port)))

        # create resource
        for i in range(resourceNb):
            resourceName = resourceNamePrefix + str(i)
            os.system("./helix-admin.sh --zkSvr %s --addResource %s %s %s %s" % (zkAddr, clusterName, resourceName, partitionNb, stateModelDef))
            if doRebalance:
                os.system("./helix-admin.sh --zkSvr %s --rebalance %s %s %s" % (zkAddr, clusterName, resourceName, partitionNb))
        os.chdir(cwd)

    @staticmethod
    def startController(clusterName, zkAddr):
        cwd = os.getcwd()
        os.chdir(TestHelper.HELIX_BIN_DIR)
        # start the controller
        os.system("./run-helix-controller.sh --zkSvr %s --cluster %s 2>&1 > /tmp/controller.log &" % (zkAddr, clusterName))
        os.chdir(cwd)

    @staticmethod
    def isOpen(ip, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((ip, int(port)))
            s.shutdown(2)
            return True
        except:
          return False



