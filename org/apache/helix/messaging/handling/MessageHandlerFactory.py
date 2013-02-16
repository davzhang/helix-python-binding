# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message


class MessageHandlerFactory:

    def createHandler(self, message, context):
        """
        Returns MessageHandler
        Parameters:
            message: Messagecontext: NotificationContext


        """
        pass


    def getMessageType(self):
        """
        Returns String


        """
        pass


    def reset(self):
        """
        Returns void


        """
        pass



