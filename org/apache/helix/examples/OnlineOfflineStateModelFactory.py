# package org.apache.helix.examples
#from org.apache.helix.examples import *
import time
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory
from org.apache.helix.util.misc import ternary

class OnlineOfflineStateModel(StateModel):



    def setDelay(self, delay):
        """
        Returns void
        Parameters:
            delay: int


        """
        _transDelay = ternary(delay > 0, delay, 0)


    def onBecomeOnlineFromOffline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("OnlineOfflineStateModel.onBecomeOnlineFromOffline()")
        self.sleep()


    def onBecomeOfflineFromOnline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("OnlineOfflineStateModel.onBecomeOfflineFromOnline()")
        self.sleep()


    def onBecomeDroppedFromOffline(self, message, context):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContext


        """
        print("OnlineOfflineStateModel.onBecomeDroppedFromOffline()")
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







#class OnlineOfflineStateModelFactory(StateModelFactory<StateModel>):
class OnlineOfflineStateModelFactory(StateModelFactory):



    """

    Parameters:
        int delay
    """
    def __init__(self, delay):
        self._delay = delay


    def createNewStateModel(self, stateUnitKey):
        """
        Returns StateModel
        Parameters:
            stateUnitKey: String
        @Override


        """
        # OnlineOfflineStateModel
        stateModel = OnlineOfflineStateModel()
        stateModel.setDelay(self._delay)
        return stateModel


