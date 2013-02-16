# package org.apache.helix
#from org.apache.helix import *
#from java.util import HashMap
#from java.util import Map
from org.apache.helix.util.misc import enum

#Type= enum('INIT', 'CALLBACK', 'FINALIZE')


class NotificationContext:
    Type= enum('INIT', 'CALLBACK', 'FINALIZE')

    """
    Java modifiers:
         final static
    Type:
        String
    """
    TASK_EXECUTOR_KEY = "TASK_EXECUTOR"

    def getEventName(self):
        """
        Returns String


        """
        return self._eventName


    def setEventName(self, eventName):
        """
        Returns void
        Parameters:
            eventName: String


        """
        _eventName = eventName


    """

    Parameters:
        HelixManager manager
    """
    def __init__(self, manager):
        self._manager = manager
#        self._map = HashMap<String, Object>()
        self._map = {}

    def getManager(self):
        """
        Returns HelixManager


        """
        return self._manager


    def getMap(self):
        """
        Returns Map<String, Object>


        """
        return self._map


    def getType(self):
        """
        Returns Type


        """
        return self._type


    def setManager(self, manager):
        """
        Returns void
        Parameters:
            manager: HelixManager


        """
        self._manager = manager


    def add(self, key, value):
        """
        Returns void
        Parameters:
            key: Stringvalue: Object


        """
#        self._map.put(key, value)
        self._map[key]=value


    def setMap(self, map):
        """
        Returns void
        Parameters:
            map: Map<String, Object>


        """
        self._map = map


    def setType(self, type):
        """
        Returns void
        Parameters:
            type: Type


        """
        self._type = type


    def get(self, key):
        """
        Returns Object
        Parameters:
            key: String


        """
        return self._map.get(key)




    def getPathChanged(self):
        """
        Returns String


        """
        return self._pathChanged


    def setPathChanged(self, pathChanged):
        """
        Returns void
        Parameters:
            pathChanged: String


        """
        self._pathChanged = pathChanged



