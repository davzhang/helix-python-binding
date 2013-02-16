# package org.apache.helix.participant.statemachine
#from org.apache.helix.participant.statemachine import *
from org.apache.helix.messaging.handling.MessageHandler import ErrorCode
from org.apache.helix.messaging.handling.MessageHandler import ErrorType


class StateTransitionError:







    """

    Parameters:
        ErrorType type
        ErrorCode code
        Exception e
    """
    def __init__(self, type, code, e):
        self._type = type
        self._code = code
        self._exception = e


    def getException(self):
        """
        Returns Exception


        """
        return _exception


    def getCode(self):
        """
        Returns ErrorCode


        """
        return _code



