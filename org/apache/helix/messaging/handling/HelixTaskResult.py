# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
#from java.util import HashMap
#from java.util import Map


class HelixTaskResult:


    def isSucess(self):
        """
        Returns boolean


        """
        return self._success


    def isInterrupted(self):
        """
        Returns boolean


        """
        return self._interrupted


    def setInterrupted(self, interrupted):
        """
        Returns void
        Parameters:
            interrupted: boolean


        """
        self._interrupted = interrupted


    def setSuccess(self, success):
        """
        Returns void
        Parameters:
            success: boolean


        """
        self._success = success


    def getMessage(self):
        """
        Returns String


        """
        return self._message


    def setMessage(self, message):
        """
        Returns void
        Parameters:
            message: String


        """
        self._message = message


    def getTaskResultMap(self):
        """
        Returns Map<String, String>


        """
        return self._taskResultMap


    def setException(self, e):
        """
        Returns void
        Parameters:
            e: Exception


        """
        _exception = e


    def getException(self):
        """
        Returns Exception


        """
        return self._exception



