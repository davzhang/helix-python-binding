# package org.apache.helix.store.zk
#from org.apache.helix.store.zk import *
#from org.apache.zookeeper.data import Stat


class PropertyItem:





    """

    Parameters:
        byte[] value
        Stat stat
    """
    def __init__(self, value, stat):
        self._value = value
        self._stat = stat


    def getBytes(self):
        """
        Returns byte[]


        """
        return _value


    def getVersion(self):
        """
        Returns int


        """
        return _stat.getVersion()


    def getLastModifiedTime(self):
        """
        Returns long


        """
        return _stat.getMtime()



