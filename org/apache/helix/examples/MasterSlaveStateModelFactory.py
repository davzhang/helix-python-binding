# package org.apache.helix.examples
#from org.apache.helix.examples import *
import time
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory

# Annotation: @SuppressWarnings("rawtypes")
from org.apache.helix.util.misc import ternary

class MasterSlaveStateModel(StateModel):

    def __init__(self):
        super(MasterSlaveStateModel,self).__init__()
        self.stateUnitKey = None
        self._transDelay = 0


    def getStateUnitKey(self):
        """
        Returns String


        """
        return self.stateUnitKey


    def setStateUnitKey(self, stateUnitKey):
        """
        Returns void
        Parameters:
            stateUnitKey: String


        """
        self.stateUnitKey = stateUnitKey


    def setDelay(self, delay):
        """
        Returns void
        Parameters:
            delay: int


        """
        _transDelay = ternary(delay > 0, delay, 0)


    def onBecomeSlaveFromOffline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("MasterSlaveStateModel.onBecomeSlaveFromOffline() for " + self.stateUnitKey)
        self.sleep()


    def sleep(self):
        """
        Returns void
        Java modifiers:
             private

        """
        try:
            time.sleep(self._transDelay)
        except Exception, e:
            print e
#            e.printStackTrace()



    def onBecomeSlaveFromMaster(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("MasterSlaveStateModel.onBecomeSlaveFromMaster() for " + self.stateUnitKey)
        self.sleep()


    def onBecomeMasterFromSlave(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("MasterSlaveStateModel.onBecomeMasterFromSlave() for " + self.stateUnitKey)
        self.sleep()


    def onBecomeOfflineFromSlave(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("MasterSlaveStateModel.onBecomeOfflineFromSlave() for " + self.stateUnitKey)
        self.sleep()


    def onBecomeDroppedFromOffline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("ObBecomeDroppedFromOffline() for " + self.stateUnitKey)
        self.sleep()


#class MasterSlaveStateModelFactory(StateModelFactory<StateModel>):
class MasterSlaveStateModelFactory(StateModelFactory):



    """

    Parameters:
        int delay
    """
    def __init__(self, delay):
        super(MasterSlaveStateModelFactory,self).__init__()
        self._delay = delay


    def createNewStateModel(self, stateUnitKey):
        """
        Returns StateModel
        Parameters:
            stateUnitKey: String
        @Override


        """
        # MasterSlaveStateModel
        stateModel = MasterSlaveStateModel()
        stateModel.setDelay(self._delay)
        stateModel.setStateUnitKey(stateUnitKey)
        return stateModel






