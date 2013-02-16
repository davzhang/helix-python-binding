# package org.apache.helix.store
#from org.apache.helix.store import *

class PropertyStat:





    """

    """
    def __init__(self):
        this(0, 0)


    """

    Parameters:
        long lastModifiedTime
        int version
    """
    def __init__(self, lastModifiedTime, version):
        _lastModifiedTime = lastModifiedTime
        _version = version


    def getLastModifiedTime(self):
        """
        Returns long


        """
        return _lastModifiedTime


    def getVersion(self):
        """
        Returns int


        """
        return _version


    def setLastModifiedTime(self, lastModifiedTime):
        """
        Returns void
        Parameters:
            lastModifiedTime: long


        """
        _lastModifiedTime = lastModifiedTime


    def setVersion(self, version):
        """
        Returns void
        Parameters:
            version: int


        """
        _version = version



