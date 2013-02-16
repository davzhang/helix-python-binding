# package org.apache.helix
#from org.apache.helix import *
from org.apache.helix.messaging.AsyncCallback import AsyncCallback
from org.apache.helix.messaging.handling.MessageHandlerFactory import MessageHandlerFactory
from org.apache.helix.model.Message import Message


class ClusterMessagingService:

    def send(self, recipientCriteria, message):
        """
        Returns int
        Parameters:
            recipientCriteria: Criteriamessage: Message


        """
        pass


    def send(self, receipientCriteria, message, callbackOnReply, timeOut):
        """
        Returns int
        Parameters:
            receipientCriteria: Criteriamessage: MessagecallbackOnReply: AsyncCallbacktimeOut: int


        """
        pass


    def send(self, receipientCriteria, message, callbackOnReply, timeOut, retryCount):
        """
        Returns int
        Parameters:
            receipientCriteria: Criteriamessage: MessagecallbackOnReply: AsyncCallbacktimeOut: intretryCount: int


        """
        pass


    def sendAndWait(self, receipientCriteria, message, callbackOnReply, timeOut):
        """
        Returns int
        Parameters:
            receipientCriteria: Criteriamessage: MessagecallbackOnReply: AsyncCallbacktimeOut: int


        """
        pass


    def sendAndWait(self, receipientCriteria, message, callbackOnReply, timeOut, retryCount):
        """
        Returns int
        Parameters:
            receipientCriteria: Criteriamessage: MessagecallbackOnReply: AsyncCallbacktimeOut: intretryCount: int


        """
        pass


    def registerMessageHandlerFactory(self, type, factory):
        """
        Returns void
        Parameters:
            type: Stringfactory: MessageHandlerFactory


        """
        pass



