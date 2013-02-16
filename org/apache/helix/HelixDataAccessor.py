# package org.apache.helix
#from org.apache.helix import *
#from java.util import List
#from java.util import Map
#from org.I0Itec.zkclient import DataUpdater


class HelixDataAccessor:

    def createProperty(self, key, value):
        """
        Returns boolean
        Parameters:
            key: PropertyKeyvalue: T


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def setProperty(self, key, value):
        """
        Returns boolean
        Parameters:
            key: PropertyKeyvalue: T


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def updateProperty(self, key, value):
        """
        Returns boolean
        Parameters:
            key: PropertyKeyvalue: T


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def getProperty(self, key):
        """
        Returns T
        Parameters:
            key: PropertyKey


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def getProperty(self, keys):
        """
        Returns List<T>
        Parameters:
            keys: List<PropertyKey>


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def removeProperty(self, key):
        """
        Returns boolean
        Parameters:
            key: PropertyKey


        """
        pass


    def getChildNames(self, key):
        """
        Returns List<String>
        Parameters:
            key: PropertyKey


        """
        pass


    def getChildValues(self, key):
        """
        Returns List<T>
        Parameters:
            key: PropertyKey


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def getChildValuesMap(self, key):
        """
        Returns Map<String, T>
        Parameters:
            key: PropertyKey


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def createChildren(self, keys, children):
        """
        Returns boolean[]
        Parameters:
            keys: List<PropertyKey>children: List<T>


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def setChildren(self, keys, children):
        """
        Returns boolean[]
        Parameters:
            keys: List<PropertyKey>children: List<T>


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def updateChildren(self, paths, updaters, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>updaters: List<DataUpdater<ZNRecord>>options: int


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def keyBuilder(self):
        """
        Returns PropertyKey.Builder


        """
        pass


    def getBaseDataAccessor(self):
        """
        Returns BaseDataAccessor<ZNRecord>


        """
        pass



