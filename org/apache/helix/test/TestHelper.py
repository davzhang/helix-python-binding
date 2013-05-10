import os


class TestHelper(object):
    # def setupCluster(String clusterName,
    #                               String zkAddr,
    #                               int startPort,
    #                               String participantNamePrefix,
    #                               String resourceNamePrefix,
    #                               int resourceNb,
    #                               int partitionNb,
    #                               int nodesNb,
    #                               int replica,
    #                               String stateModelDef,
    #                               boolean doRebalance)
    HELIX_BIN_DIR="/home/dzhang/project/incubator-helix/helix-core/target/helix-core-pkg/bin"

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
        cwd = os.getcwd()
        os.chdir(TestHelper.HELIX_BIN_DIR)
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

    @staticmethod
    def startController(clusterName, zkAddr):
        # start the controller
        os.system("./run-helix-controller.sh --zkSvr %s --cluster %s 2>&1 > /tmp/controller.log &" % (zkAddr, clusterName))




