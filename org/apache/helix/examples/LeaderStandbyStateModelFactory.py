# package org.apache.helix.examples
#from org.apache.helix.examples import *
import time
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message
from org.apache.helix.participant.statemachine.StateModel import StateModel
from org.apache.helix.participant.statemachine.StateModelFactory import StateModelFactory
from org.apache.helix.util.misc import ternary


#class LeaderStandbyStateModelFactory(StateModelFactory<StateModel>):
class LeaderStandbyStateModelFactory(StateModelFactory):


    """

    Parameters:
        int delay
    """
    def __init__(self, delay):
        self._delay = delay
#        self._StateModel = StateModel # this is a class to to extended from



    def createNewStateModel(self, stateUnitKey):
        """
        Returns StateModel
        Parameters:
            stateUnitKey: String
        @Override
        """

        class LeaderStandbyStateModel(StateModel):


            def setDelay(self, delay):
                """
                Returns void
                Parameters:
                delay: int


                """
                self._transDelay =ternary(delay > 0, delay, 0)


            def onBecomeLeaderFromStandby(self, message, context):
                """
                Returns void
                Parameters:
                message: Messagecontext: NotificationContext


                """
                print("LeaderStandbyStateModel.onBecomeLeaderFromStandby()")
                self.sleep()


            def onBecomeStandbyFromLeader(self, message, context):
                """
                Returns void
                Parameters:
                message: Messagecontext: NotificationContext


                """
                print ("LeaderStandbyStateModel.onBecomeStandbyFromLeader()")
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
#                    e.printStackTrace()

        # LeaderStandbyStateModel
        stateModel = LeaderStandbyStateModel()
        stateModel.setDelay(self._delay)
        return stateModel







