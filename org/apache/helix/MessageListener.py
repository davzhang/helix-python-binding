# package org.apache.helix
#from org.apache.helix import *
#from java.util import List
from org.apache.helix.model.Message import Message


class MessageListener:

    def onMessage(self, instanceName, messages, changeContext):
        """
        Returns void
        Parameters:
            instanceName: Stringmessages: List<Message>changeContext: NotificationContext


        """
        pass



