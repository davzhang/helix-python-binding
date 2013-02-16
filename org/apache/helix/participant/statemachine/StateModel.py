# package org.apache.helix.participant.statemachine
#from org.apache.helix.participant.statemachine import *
#from org.apache.log4j import Logger
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message

from org.apache.helix.util.logger import get_logger

class StateModel(object):

    """
    Java modifiers:
         final static
    Type:
        String
    """
    DEFAULT_INITIAL_STATE = "OFFLINE"

    logger = get_logger(__name__)

    def __init__(self):
        self._currentState = StateModel.DEFAULT_INITIAL_STATE;


    def getCurrentState(self):
        """
        Returns String


        """
        return self._currentState


    def defaultTransitionHandler(self):
        """
        Returns void


        """
        self.logger.error("Default default handler. The idea is to invoke this if no transition method is found. Yet to be implemented")


    def updateState(self, newState):
        """
        Returns boolean
        Parameters:
            newState: String


        """
        _currentState = newState
        return True


    def rollbackOnError(self, message, context, error):
        """
        Returns void
        Parameters:
            message: Messagecontext: NotificationContexterror: StateTransitionError


        """
        self.logger.error("Default rollback method invoked on error. Error Code:" + str(error.getCode()))


    def reset(self):
        """
        Returns void


        """
        self.logger.warn("Default reset method invoked. Either because the process longer own this resource or session timedout")



