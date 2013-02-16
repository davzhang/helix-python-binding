# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum

ErrorType=enum('FRAMEWORK', 'INTERNAL')
ErrorCode=enum('ERROR', 'CANCEL', 'TIMEOUT')

class MessageHandler(object):

    """

    Parameters:
        Message message
        NotificationContext context
    """
    def __init__(self, message, context):
        self._message = message
        self._notificationContext = context


    def handleMessage(self):
        """
        Returns HelixTaskResult
        Java modifiers:
             abstract

        Throws: 
            InterruptedException
        """
        pass


    def onError(self, e, code, type):
        """
        Returns void
        Parameters:
            e: Exceptioncode: ErrorCodetype: ErrorType
        Java modifiers:
             abstract

        """
        pass


    def onTimeout(self):
        """
        Returns void


        """



