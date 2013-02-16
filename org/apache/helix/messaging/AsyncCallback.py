# package org.apache.helix.messaging
#from org.apache.helix.messaging import *
#from java.util import ArrayList
#from java.util import Date
#from java.util import List
#from java.util import Timer
#from java.util import TimerTask
#from org.apache.log4j import Logger
import time
from org.apache.helix.model.Message import Message

from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum

class AsyncCallback:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    _logger = get_logger(__name__)


    """

    Parameters:
        long timeout
    """
    def __init__(self, timeout=-1):
        self._logger.info("Setting time out to " + timeout + " ms")
        self._timeout = timeout
        self._startTimeStamp = 0
        self._messagesSent = []
        self._messageReplied = []
        self._timedOut = False
        self._isInterrupted = False
#        Timer self._timer = null;
#        List<Message> self._messagesSent;
#        protected final List<Message> self._messageReplied = new ArrayList<Message>();
#        boolean self._timedOut = false;
#        boolean self._isInterrupted = false;



    def setTimeout(self, timeout):
        """
        Returns void
        Parameters:
            timeout: long
        Java modifiers:
             final

        """
        self._logger.info("Setting time out to " + timeout + " ms")
        self._timeout = timeout


    def getMessageReplied(self):
        """
        Returns List<Message>


        """
        return self._messageReplied


    def isInterrupted(self):
        """
        Returns boolean


        """
        return self._isInterrupted


    def setInterrupted(self, b):
        """
        Returns void
        Parameters:
            b: boolean


        """
        self._isInterrupted = True


    def onReply(self, message):
        """
        Returns void
        Parameters:
            message: Message
        Java modifiers:
             final synchronized

        """
        self._logger.info("OnReply msg " + message.getMsgId())
        if not self.isDone():
            self._messageReplied.add(message)
            try:
                self.onReplyMessage(message)
            except Exception, e:
                self._logger.error(e)


        if self.isDone():
            if self._timer != None:
                self._timer.cancel()

            self.notifyAll()



    def isDone(self):
        """
        Returns boolean


        """
        return self._messageReplied.size() == self._messagesSent.size()


    def isTimedOut(self):
        """
        Returns boolean


        """
        return self._timedOut


    def setMessagesSent(self, generatedMessage):
        """
        Returns void
        Parameters:
            generatedMessage: List<Message>
        Java modifiers:
             final

        """
        self._messagesSent = generatedMessage


    def startTimer(self):
        """
        Returns void
        Java modifiers:
             final

        """
        if self._timer == None and self._timeout > 0:
            if self._startTimeStamp == 0: 
#                self._startTimeStamp = Date().getTime()
                self._startTimeStamp = time.time()
             #TODO: fix timer
#            self._timer = Timer(True)
#            self._timer.schedule(TimeoutTask(self), self._timeout)



    def onTimeOut(self):
        """
        Returns void
        Java modifiers:
             abstract

        """
        pass


    def onReplyMessage(self, message):
        """
        Returns void
        Parameters:
            message: Message
        Java modifiers:
             abstract

        """
        pass



#    class TimeoutTask(TimerTask):
#
#
#
#        """
#
#        Parameters:
#            AsyncCallback asyncCallback
#        """
#        def __init__(self, asyncCallback):
#            self._callback = asyncCallback
#
#
#        def run(self):
#            """
#            Returns void
#            @Override
#
#
#            """
#            try:
#                #TODO
##                synchronized (_callback)
#                    self._callback.self._timedOut = True
#                    self._callback.notifyAll()
#                    self._callback.onTimeOut()
#
#            except Exception, e:
#                self._logger.error(e)
#
#
#
#
#
