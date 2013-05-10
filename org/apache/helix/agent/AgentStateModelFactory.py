# package org.apache.helix.agent
from org.apache.helix.agent.AgentStateModel import AgentStateModel
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory


# class AgentStateModelFactory(StateModelFactory<AgentStateModel>):
class AgentStateModelFactory(StateModelFactory):

    def createNewStateModel(self, partitionKey):
        """
        Returns AgentStateModel
        Parameters:
            partitionKey: String
        @Override


        """
        # AgentStateModel
        model = AgentStateModel()
        return model



