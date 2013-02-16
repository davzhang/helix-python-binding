# package org.apache.helix.examples
#from org.apache.helix.examples import *
#from java.io import File
#from org.apache.commons.cli import CommandLine
#from org.apache.commons.cli import CommandLineParser
#from org.apache.commons.cli import GnuParser
#from org.apache.commons.cli import HelpFormatter
#from org.apache.commons.cli import Option
#from org.apache.commons.cli import OptionBuilder
#from org.apache.commons.cli import OptionGroup
#from org.apache.commons.cli import Options
#from org.apache.commons.cli import ParseException
# package org.apache.helix.examples
import threading

from org.apache.helix.HelixManager import HelixManager
#import org.apache.helix.HelixManager

from org.apache.helix.HelixManagerFactory import HelixManagerFactory
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.examples.LeaderStandbyStateModelFactory import LeaderStandbyStateModelFactory
from org.apache.helix.examples.MasterSlaveStateModelFactory import MasterSlaveStateModelFactory
from org.apache.helix.examples.OnlineOfflineStateModelFactory import OnlineOfflineStateModelFactory
from org.apache.helix.model.Message import MessageType
from org.apache.helix.participant.StateMachineEngine import StateMachineEngine
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory
#from org.apache.helix.tools.ClusterStateVerifier import ClusterStateVerifier
from optparse import OptionParser, OptionGroup
import os, sys

class ExampleProcess:

    """
    Java modifiers:
         final static
    Type:
        String
    """
    zkServer = "zkSvr"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    cluster = "cluster"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    hostAddress = "host"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    hostPort = "port"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    relayCluster = "relayCluster"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    help = "help"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    configFile = "configFile"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    stateModel = "stateModelType"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    transDelay = "transDelay"


    """

    Parameters:
        String zkConnectString
        String clusterName
        String instanceName
        String file
        String stateModel
        int delay
    """
    def __init__(self, zkConnectString, clusterName, instanceName, file, stateModel, delay):
        self.zkConnectString = zkConnectString
        self.clusterName = clusterName
        self.instanceName = instanceName
        self._file = file
        self.stateModelType = stateModel
        self.delay = delay


    def start(self):
        """
        Returns void


        Throws: 
            Exception
        """
        if self._file == None:
            manager = HelixManagerFactory.getZKHelixManager(self.clusterName, self.instanceName, InstanceType.PARTICIPANT, self.zkConnectString)
        else:
            manager = HelixManagerFactory.getStaticFileHelixManager(self.clusterName, self.instanceName, InstanceType.PARTICIPANT, self._file)

        if "MasterSlave".upper() == self.stateModelType.upper():
            stateModelFactory = MasterSlaveStateModelFactory(self.delay)
        else:
            if "OnlineOffline".upper() == self.stateModelType.upper():
                stateModelFactory = OnlineOfflineStateModelFactory(self.delay)
            else:
                if "LeaderStandby".upper() == self.stateModelType.upper():
                    stateModelFactory = LeaderStandbyStateModelFactory(self.delay)

        # StateMachineEngine
        stateMach = manager.getStateMachineEngine()
        stateMach.registerStateModelFactory(self.stateModelType, stateModelFactory)
        manager.connect()
        manager.getMessagingService().registerMessageHandlerFactory(MessageType.toString(MessageType.STATE_TRANSITION), stateMach)
#        if self._file != None:
#            ClusterStateVerifier.verifyFileBasedClusterStates(self._file, self.instanceName, stateModelFactory)



def constructCommandLineOptions(parser):
    """
    Returns Options
    # Annotation: @SuppressWarnings("static-access")

    Java modifiers:
         private static

    """
    # Option
    #        global options
    #        helpOptioggnn = OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info").create()
    # Option
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("--zkSvr", action="store", dest="zkServer", default="localhost:2181", help="zookeeper address [default: %default]")
    #        zkServerOption = OptionBuilder.withLongOpt(zkServer).withDescription("Provide zookeeper address").create()
    #        zkServerOption.setArgs(1)
    #        zkServerOption.setRequired(True)
    #        zkServerOption.setArgName("ZookeeperServerAddress(Required)")
    # Option
    parser.add_option("--cluster", action="store", dest="cluster", default="storage-integration-cluster", help = "Cluster name [default: %default]")
    #        clusterOption = OptionBuilder.withLongOpt(cluster).withDescription("Provide cluster name").create()
    #        clusterOption.setArgs(1)
    #        clusterOption.setRequired(True)
    #        clusterOption.setArgName("Cluster name (Required)")
    # Option"
    parser.add_option("--host", action="store", dest="hostAddress", default="localhost", help = "Host name [default: %default]")
    #        hostOption = OptionBuilder.withLongOpt(hostAddress).withDescription("Provide host name").create()
    #        hostOption.setArgs(1)
    #        hostOption.setRequired(True)
    #        hostOption.setArgName("Host name (Required)")
    # Option
    parser.add_option("--port", action="store", dest="hostPort", default=8905, help = "port [default: %default]")
    #        portOption = OptionBuilder.withLongOpt(hostPort).withDescription("Provide host port").create()
    #        portOption.setArgs(1)
    #        portOption.setRequired(True)
    #        portOption.setArgName("Host port (Required)")
    # Option
    parser.add_option("--stateModelType", action="store", dest="stateModel", default="MasterSlave", help = "State Model Type [default: %default]")
    #        stateModelOption = OptionBuilder.withLongOpt(stateModel).withDescription("StateModel Type").create()
    #        stateModelOption.setArgs(1)
    #        stateModelOption.setRequired(True)
    #        stateModelOption.setArgName("StateModel Type (Required)")
    # Option
    parser.add_option("--configFile", action="store", dest="configFile", default=None, help = "File to read states/messages [default: %default]")
    #        fileOption = OptionBuilder.withLongOpt(configFile).withDescription("Provide file to read states/messages").create()
    #        fileOption.setArgs(1)
    #        fileOption.setRequired(True)
    #        fileOption.setArgName("File to read states/messages (Optional)")
    # Option
    parser.add_option("--transDelay", action="store", dest="transDelay", default=None, help = "Delay time in state transition, in MS [default: %default]")
    #        transDelayOption = OptionBuilder.withLongOpt(transDelay).withDescription("Provide state trans delay").create()
    #        transDelayOption.setArgs(1)
    #        transDelayOption.setRequired(False)
    #        transDelayOption.setArgName("Delay time in state transition, in MS")
    # OptionGroup
    #        parser.add_option("--cluster", action="store", dest="cluster", default="storage-integration-cluster", help = "Cluster name [default: %default]")
    #        optionGroup = OptionGroup()
    #        optionGroup.addOption(zkServerOption)
    #        optionGroup.addOption(fileOption)
    #        # Options
    #        options = Options()
    #        options.addOption(helpOption)
    #        options.addOption(clusterOption)
    #        options.addOption(hostOption)
    #        options.addOption(portOption)
    #        options.addOption(stateModelOption)
    #        options.addOption(transDelayOption)
    #        options.addOptionGroup(optionGroup)
    #        return options
    return parser


    #    def printUsage(cliOptions):
    #        """
    #        Returns void
    #        Parameters:
    #            cliOptions: Options
    #        Java modifiers:
    #             static
    #
    #        """
    #        # HelpFormatter
    #        helpFormatter = HelpFormatter()
    #        helpFormatter.printHelp("java " + ExampleProcess.class.getName(), cliOptions)


def processCommandLineArgs(argv):
    """
    Returns CommandLine
    Parameters:
        cliArgs: String[]
    Java modifiers:
         static

    Throws:
        Exception
    """
    # CommandLineParser
    #        cliParser = GnuParser()
    # Options
    parser = constructCommandLineOptions(argv)

    (options, args) = parser.parse_args()
    return options

    #        try:
    #            return cliParser.parse(cliOptions, cliArgs)
    #        except ParseException, pe:
    #            System.err.println("CommandLineClient: failed to parse command-line options: " + pe.toString())
    #            printUsage(cliOptions)
    #            System.exit(1)
    #
    #        return None


def main(args):
    """
    Returns void
    Parameters:
        args: String[]
    Java modifiers:
         static

    Throws:
        Exception
    """
    # String
    zkConnectString = "localhost:2181"
    # String
    clusterName = "storage-integration-cluster"
    # String
    instanceName = "localhost_8905"
    # String
    file = None
    # String
    stateModelValue = "MasterSlave"
    # int
    delay = 0
    # boolean
    skipZeroArgs = True
    if not skipZeroArgs or len(args) > 0:
        # CommandLine
        options = processCommandLineArgs(args)
        zkConnectString = options.zkServer
    #            zkConnectString = cmd.getOptionValue(zkServer)
        clusterName = options.cluster
    #            clusterName = cmd.getOptionValue(cluster)
        # String
        host = options.hostAddress
    #            host = cmd.getOptionValue(hostAddress)
        # String
    #            portString = cmd.getOptionValue(hostPort)
        # int
        port = int(options.hostPort)
        instanceName = host + "_" + str(port)
        file = options.configFile
    #            file = cmd.getOptionValue(configFile)
        if file != None:
            # File
            if not os.path.exists(file):
                print("static config file doesn't exist")
                exit(1)


        stateModelValue = options.stateModel
    #            stateModelValue = cmd.getOptionValue(stateModel)
    #            if cmd.hasOption(transDelay):
        if options.transDelay:
            try:
                delay = int(options.transDelay)
                if delay < 0:
                    raise Exception("delay must be positive")

            except Exception, e:
                print e
                delay = 0



    print("Starting Process with ZK:" + zkConnectString)
    # ExampleProcess
    process = ExampleProcess(zkConnectString, clusterName, instanceName, file, stateModelValue, delay)
    process.start()

    for thread in threading.enumerate():
        if thread is not threading.currentThread():
            thread.join()
#    threading.currentThread().join()

if __name__ == "__main__":
    main(sys.argv[1:])




