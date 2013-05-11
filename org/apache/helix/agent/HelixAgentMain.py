# package org.apache.helix.agent
#from org.apache.helix.agent import *
#from java.util import Arrays
#from org.apache.commons.cli import CommandLine
#from org.apache.commons.cli import CommandLineParser
#from org.apache.commons.cli import GnuParser
#from org.apache.commons.cli import HelpFormatter
#from org.apache.commons.cli import Option
#from org.apache.commons.cli import OptionBuilder
#from org.apache.commons.cli import Options
#from org.apache.commons.cli import ParseException
#from org.apache.helix.HelixManager import HelixManager
#from org.apache.helix import InstanceType
#from org.apache.helix.manager.zk import ZKHelixManager
#from org.apache.helix.participant import StateMachineEngine
#from org.apache.log4j import Logger
from optparse import OptionParser
import threading
import sys
from org.apache.helix.HelixManagerFactory import HelixManagerFactory
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.agent.AgentStateModelFactory import AgentStateModelFactory

from org.apache.helix.util.logger import get_logger

class HelixAgentMain(object):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    """
    Java modifiers:
         final static
    Type:
        String
    """
    zkAddr = "zkSvr"

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
    help = "help"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    instanceName = "instanceName"

    """
    Java modifiers:
         final static
    Type:
        String
    """
    stateModel = "stateModel"

def constructCommandLineOptions(argv):
    """
    Returns Options
    # Annotation: @SuppressWarnings("static-access")

    Java modifiers:
         private static synchronized

    """
    parser = OptionParser(usage="usage: %prog [options]")

    # Option
    # helpOption = OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info").create();
    # Option
    # zkAddrOption = OptionBuilder.withLongOpt(zkAddr).hasArgs(1).isRequired(True).withArgName("ZookeeperServerAddress(Required)").withDescription("Provide zookeeper address").create();
    parser.add_option("--zkAddr", action="store", dest="zkAddr", default="localhost:2181", help="Provide zookeeper address [default: %default]")
    # Option
    # clusterOption = OptionBuilder.withLongOpt(cluster).hasArgs(1).isRequired(True).withArgName("Cluster name (Required)").withDescription("Provide cluster name").create();
    parser.add_option("--cluster", action="store", dest="cluster", default="CLUSTER", help="cluster name [default: %default]")
    # Option
    # instanceNameOption = OptionBuilder.withLongOpt(instanceName).hasArgs(1).isRequired(True).withArgName("Helix agent name (Required)").withDescription("Provide Helix agent name").create();
    parser.add_option("--instanceName", action="store", dest="instanceName", default="instance", help="Helix agent name [default: %default]")
    # Option
    # stateModelOption = OptionBuilder.withLongOpt(stateModel).hasArgs(1).isRequired(True).withArgName("State model name (Required)").withDescription("Provide state model name").create();
    parser.add_option("--stateModel", action="store", dest="stateModel", default="MasterSlave", help="State model name name [default: %default]")
    return parser


def processCommandLineArgs(argv):
    parser = constructCommandLineOptions(argv)

    (options, args) = parser.parse_args()
    return options

    # cliParser = GnuParser();
    # # Options
    # cliOptions = constructCommandLineOptions();
    # try:
    #     return cliParser.parse(cliOptions, cliArgs)
    # except ParseException, pe:
    #     LOG.error("fail to parse command-line options. cliArgs: " + Arrays.toString(cliArgs), pe);
    #     printUsage(cliOptions);
    #     System.exit(1);
    #
    # return None


# class HelixAgentShutdownHook(Thread):
#
#
#
#     """
#
#     Parameters:
#         HelixManager manager
#     """
#     def __init__(self, manager):
#         _manager = manager;
#
#
#     def run(self):
#         """
#         Returns void
#         @Override
#
#
#         """
#         LOG.info("HelixAgentShutdownHook invoked. agent: " + _manager.getInstanceName());
#         if _manager != None && _manager.isConnected():
#             _manager.disconnect();



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
    # CommandLine
    options = processCommandLineArgs(args)
    # String
    zkAddress = options.zkAddr
    # String
    clusterName = options.cluster
    # String
    instance = options.instanceName
    # String
    stateModelName = options.stateModel

    # HelixManager
    # manager = ZKHelixManager(clusterName, instance, InstanceType.PARTICIPANT, zkAddress);
    manager = HelixManagerFactory.getZKHelixManager(clusterName, instance, InstanceType.PARTICIPANT, zkAddress)

    # StateMachineEngine

    stateMach = manager.getStateMachineEngine()
    stateMach.registerStateModelFactory(stateModelName, AgentStateModelFactory())
    # Runtime.getRuntime().addShutdownHook(HelixAgentShutdownHook(manager))
    manager.connect()

    # try:
    #     manager.connect();
    #     Thread.currentThread().join();
    # except Exception, e:
    #     LOG.error(e);
    # final:
    #         if manager != None && manager.isConnected():
    #             manager.disconnect();

    # TODO: join here OK?
    for thread in threading.enumerate():
        if thread is not threading.currentThread():
            thread.join()


if __name__ == "__main__":
    main(sys.argv[1:])

