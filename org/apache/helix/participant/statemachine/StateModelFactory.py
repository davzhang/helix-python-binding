# package org.apache.helix.participant.statemachine
#from org.apache.helix.participant.statemachine import *
#from java.util import Map
#from java.util.concurrent import ConcurrentHashMap
#from java.util.concurrent import ConcurrentMap

#Java modifiers:
#     abstract
# Parameterized type: <T extends StateModel>
class StateModelFactory(object):


    def __init__(self):
        self._stateModelMap = {}  # new ConcurrentHashMap<String, T>();

    def createNewStateModel(self, partitionName):
        """
        Returns T
        Parameters:
            partitionName: String
        Java modifiers:
             abstract

        """
        pass


    def addStateModel(self, partitionName, stateModel):
        """
        Returns void
        Parameters:
            partitionName: StringstateModel: T


        """
        self._stateModelMap.__setitem__(partitionName, stateModel)


    def createAndAddStateModel(self, partitionName):
        """
        Returns void
        Parameters:
            partitionName: String


        """
        self._stateModelMap.__setitem__(partitionName, self.createNewStateModel(partitionName))


    def getStateModel(self, partitionName):
        """
        Returns T
        Parameters:
            partitionName: String


        """
        return self._stateModelMap.get(partitionName)


    def getStateModelMap(self):
        """
        Returns Map<String, T>


        """
        return self._stateModelMap



