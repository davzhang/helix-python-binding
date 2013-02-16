# package org.apache.helix.participant
#from org.apache.helix.participant import *
#from java.util import List
from org.apache.helix.HelixConstants import ChangeType
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory


class GenericLeaderStandbyStateModelFactory(StateModelFactory<GenericLeaderStandbyModel>):





    """

    Parameters:
        CustomCodeCallbackHandler callback
        List<ChangeType> notificationTypes
    """
    def __init__(self, callback, notificationTypes):
        if callback == None or notificationTypes == None or notificationTypes.size() == 0: 
            raise IllegalArgumentException("Require: callback | notificationTypes")


        self._callback = callback
        self._notificationTypes = notificationTypes


    def createNewStateModel(self, partitionKey):
        """
        Returns GenericLeaderStandbyModel
        Parameters:
            partitionKey: String
        @Override


        """
        return GenericLeaderStandbyModel(_callback, _notificationTypes, partitionKey)



