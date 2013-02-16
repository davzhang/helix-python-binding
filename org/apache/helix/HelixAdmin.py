# package org.apache.helix
#from org.apache.helix import *
#from java.io import IOException
#from java.util import List
#from java.util import Map
#from java.util import Set
from org.apache.helix.ConfigScope import ConfigScopeProperty
from org.apache.helix.model.ExternalView import ExternalView
from org.apache.helix.model.IdealState import IdealState
from org.apache.helix.model.InstanceConfig import InstanceConfig
from org.apache.helix.model.StateModelDefinition import StateModelDefinition


class HelixAdmin:

    def getClusters(self):
        """
        Returns List<String>


        """
        pass


    def getInstancesInCluster(self, clusterName):
        """
        Returns List<String>
        Parameters:
            clusterName: String


        """
        pass


    def getInstanceConfig(self, clusterName, instanceName):
        """
        Returns InstanceConfig
        Parameters:
            clusterName: StringinstanceName: String


        """
        pass


    def getResourcesInCluster(self, clusterName):
        """
        Returns List<String>
        Parameters:
            clusterName: String


        """
        pass


    def addCluster(self, clusterName, overwritePrevRecord):
        """
        Returns void
        Parameters:
            clusterName: StringoverwritePrevRecord: boolean


        """
        pass


    def addClusterToGrandCluster(self, clusterName, grandCluster):
        """
        Returns void
        Parameters:
            clusterName: StringgrandCluster: String


        """
        pass


    def addResource(self, clusterName, resourceName, numResources, stateModelRef):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: StringnumResources: intstateModelRef: String


        """
        pass


    def addResource(self, clusterName, resourceName, numResources, stateModelRef, idealStateMode):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: StringnumResources: intstateModelRef: StringidealStateMode: String


        """
        pass


    def addResource(self, clusterName, resourceName, numResources, stateModelRef, idealStateMode, bucketSize):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: StringnumResources: intstateModelRef: StringidealStateMode: StringbucketSize: int


        """
        pass


    def addInstance(self, clusterName, instanceConfig):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceConfig: InstanceConfig


        """
        pass


    def dropInstance(self, clusterName, instanceConfig):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceConfig: InstanceConfig


        """
        pass


    def getResourceIdealState(self, clusterName, dbName):
        """
        Returns IdealState
        Parameters:
            clusterName: StringdbName: String


        """
        pass


    def setResourceIdealState(self, clusterName, resourceName, idealState):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: StringidealState: IdealState


        """
        pass


    def enableInstance(self, clusterName, instanceName, enabled):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceName: Stringenabled: boolean


        """
        pass


    def enablePartition(self, enabled, clusterName, instanceName, resourceName, partitionNames):
        """
        Returns void
        Parameters:
            enabled: booleanclusterName: StringinstanceName: StringresourceName: StringpartitionNames: List<String>


        """
        pass


    def enableCluster(self, clusterName, enabled):
        """
        Returns void
        Parameters:
            clusterName: Stringenabled: boolean


        """
        pass


    def resetPartition(self, clusterName, instanceName, resourceName, partitionNames):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceName: StringresourceName: StringpartitionNames: List<String>


        """
        pass


    def resetInstance(self, clusterName, instanceNames):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceNames: List<String>


        """
        pass


    def resetResource(self, clusterName, resourceNames):
        """
        Returns void
        Parameters:
            clusterName: StringresourceNames: List<String>


        """
        pass


    def addStateModelDef(self, clusterName, stateModelDef, record):
        """
        Returns void
        Parameters:
            clusterName: StringstateModelDef: Stringrecord: StateModelDefinition


        """
        pass


    def dropResource(self, clusterName, resourceName):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: String


        """
        pass


    def addStat(self, clusterName, statName):
        """
        Returns void
        Parameters:
            clusterName: StringstatName: String


        """
        pass


    def addAlert(self, clusterName, alertName):
        """
        Returns void
        Parameters:
            clusterName: StringalertName: String


        """
        pass


    def dropStat(self, clusterName, statName):
        """
        Returns void
        Parameters:
            clusterName: StringstatName: String


        """
        pass


    def dropAlert(self, clusterName, alertName):
        """
        Returns void
        Parameters:
            clusterName: StringalertName: String


        """
        pass


    def getStateModelDefs(self, clusterName):
        """
        Returns List<String>
        Parameters:
            clusterName: String


        """
        pass


    def getStateModelDef(self, clusterName, stateModelName):
        """
        Returns StateModelDefinition
        Parameters:
            clusterName: StringstateModelName: String


        """
        pass


    def getResourceExternalView(self, clusterName, resourceName):
        """
        Returns ExternalView
        Parameters:
            clusterName: StringresourceName: String


        """
        pass


    def dropCluster(self, clusterName):
        """
        Returns void
        Parameters:
            clusterName: String


        """
        pass


    def setConfig(self, scope, properties):
        """
        Returns void
        Parameters:
            scope: ConfigScopeproperties: Map<String, String>


        """
        pass


    def removeConfig(self, scope, keys):
        """
        Returns void
        Parameters:
            scope: ConfigScopekeys: Set<String>


        """
        pass


    def getConfig(self, scope, keys):
        """
        Returns Map<String, String>
        Parameters:
            scope: ConfigScopekeys: Set<String>


        """
        pass


    def getConfigKeys(self, scope, clusterName, keys):
        """
        Returns List<String>
        Parameters:
            scope: ConfigScopePropertyclusterName: Stringkeys: String


        """
        pass


    def rebalance(self, clusterName, resourceName, replica):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: Stringreplica: int


        """
        pass


    def addIdealState(self, clusterName, resourceName, idealStateFile):
        """
        Returns void
        Parameters:
            clusterName: StringresourceName: StringidealStateFile: String


        Throws: 
            IOException
        """
        pass


    def addStateModelDef(self, clusterName, stateModelDefName, stateModelDefFile):
        """
        Returns void
        Parameters:
            clusterName: StringstateModelDefName: StringstateModelDefFile: String


        Throws: 
            IOException
        """
        pass


    def addMessageConstraint(self, clusterName, constraintId, constraints):
        """
        Returns void
        Parameters:
            clusterName: StringconstraintId: Stringconstraints: Map<String, String>


        """
        pass



