'''
  Create a sample web API to return external view and refresh upon changes
  Start a Spectator and register a external view listener

  Requirement:
    Install bottle package
     easy_install -U bottle
'''
import json
from optparse import OptionParser
import bottle
from bottle import route, run
import time
import sys
from org.apache.helix.HelixManagerFactory import HelixManagerFactory
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.ExternalViewChangeListener import ExternalViewChangeListener
from org.apache.helix.examples.MasterSlaveStateModelFactory import MasterSlaveStateModelFactory

class HelixEVListener(ExternalViewChangeListener):
    def __init__(self):
        self.externalViewList = {}
        self.externalViewChangeTimeStamp = 0

    def onExternalViewChange(self, externalViewList, changeContext):
        self.externalViewChangeTimeStamp = str(time.time())
        self.externalViewList = externalViewList

    def getResources(self):
        ''' get the resources
        '''
        if not self.externalViewList: return []
        return [ev.getResourceName() for ev in self.externalViewList]

    def getNumPartitions(self):
        if not self.externalViewList: return []
        return [len(ev._record.getMapFields()) for ev in self.externalViewList]

    def getTimeStamp(self):
        return self.externalViewChangeTimeStamp

    def getEV(self, resourceName):
        resources = self.getResources()
        if resourceName not in resources: return None
#        return json.dumps(self.externalViewList[resourceName]._record.getMapFields())
        return self.externalViewList[resources.index(resourceName)]._record.getMapFields()

helixEVListener = HelixEVListener()

@route('/resources', method='GET')
def get_resources():
    return dict(resources=helixEVListener.getResources())

@route('/partitions', method='GET')
def get_partitions():
    return dict(partitions = helixEVListener.getNumPartitions())

@route('/externalview/:resource', method='GET')
def get_partitions(resource):
    return dict(ev = helixEVListener.getEV(resource))

@route('/gettimestamp', method='GET')
def get_partitions():
    return dict(timestamp = helixEVListener.getTimeStamp())

def constructCommandLineOptions(parser):
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("--zkSvr", action="store", dest="zkServer", default="localhost:2181", help="zookeeper address [default: %default]")
    parser.add_option("--cluster", action="store", dest="cluster", default="storage-integration-cluster", help = "Cluster name [default: %default]")
    return parser


def setupHelixEVListening(args):
    parser = constructCommandLineOptions(args)
    (options, args) = parser.parse_args()
    zkConnectString = options.zkServer
    clusterName = options.cluster
    instanceName = "dummy"
    manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectString)

    manager.connect()
    manager.addExternalViewChangeListener(helixEVListener)

def main(args):
    setupHelixEVListening(args)
    bottle.debug(True)
    run()

if __name__ == "__main__":
    main(sys.argv[1:])


