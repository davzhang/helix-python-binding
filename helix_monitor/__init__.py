''' a webapp to display and monitor Helix status
    Use flask
'''
from org.apache.helix import HelixManagerFactory
from org.apache.helix.InstanceType import InstanceType
from org.apache.helix.examples.ExampleEVListener import ExampleEVListener

__author__ = 'dzhang'



from helix_monitor.create_app_helper import create_app
from org.apache.helix.HelixManagerFactory import HelixManagerFactory

app = create_app(__name__)
helixEVListener = ExampleEVListener()

import helix_monitor.views

# setup helix

def setupHelixSpectator(zkConnectString, clusterName):
    instanceName = "dummy"
    manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectString)

    manager.connect()
    manager.addExternalViewChangeListener(helixEVListener)
