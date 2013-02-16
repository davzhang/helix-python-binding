# package org.apache.helix.participant
#from org.apache.helix.participant import *
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory


class DistClusterControllerStateModelFactory(StateModelFactory<DistClusterControllerStateModel>):



    """

    Parameters:
        String zkAddr
    """
    def __init__(self, zkAddr):
        self._zkAddr = zkAddr


    def createNewStateModel(self, stateUnitKey):
        """
        Returns DistClusterControllerStateModel
        Parameters:
            stateUnitKey: String
        @Override


        """
        return DistClusterControllerStateModel(_zkAddr)



