# package org.apache.helix.participant
#from org.apache.helix.participant import *
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory


class StateMachineEngine(MessageHandlerFactory):

#    def registerStateModelFactory(self, stateModelDef, factory):
#        """
#        Returns boolean
#        Parameters:
#            stateModelDef: Stringfactory: StateModelFactory<? extends StateModel>
#
#
#        """
#        pass


    def registerStateModelFactory(self, stateModelDef, factory, factoryName="StateModelFactor"):
        """
        Returns boolean
        Parameters:
            stateModelDef: Stringfactory: StateModelFactory<? extends StateModel>factoryName: String


        """
        pass


#unsupported as in HelixStateMachineEngine.py
#    def removeStateModelFactory(self, stateModelDef, factory):
#        """
#        Returns boolean
#        Parameters:
#            stateModelDef: Stringfactory: StateModelFactory<? extends StateModel>
#
#
#        """
#        pass
#
#
#    def removeStateModelFactory(self, stateModelDef, factory, factoryName):
#        """
#        Returns boolean
#        Parameters:
#            stateModelDef: Stringfactory: StateModelFactory<? extends StateModel>factoryName: String
#
#
#        """
#        pass



