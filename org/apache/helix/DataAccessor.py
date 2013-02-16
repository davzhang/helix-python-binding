# package org.apache.helix
#from org.apache.helix import *
#from java.util import List
#from java.util import Map

#@Deprecated

class DataAccessor:

    def setProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: ZNRecordkeys: String


        """
        pass


    def setProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: HelixPropertykeys: String


        """
        pass


    def updateProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: ZNRecordkeys: String


        """
        pass


    def updateProperty(self, type, value, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypevalue: HelixPropertykeys: String


        """
        pass


    def getProperty(self, type, keys):
        """
        Returns ZNRecord
        Parameters:
            type: PropertyTypekeys: String


        """
        pass


    def getProperty(self, clazz, type, keys):
        """
        Returns T
        Parameters:
            clazz: Class<T>type: PropertyTypekeys: String


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def removeProperty(self, type, keys):
        """
        Returns boolean
        Parameters:
            type: PropertyTypekeys: String


        """
        pass


    def getChildNames(self, type, keys):
        """
        Returns List<String>
        Parameters:
            type: PropertyTypekeys: String


        """
        pass


    def getChildValues(self, type, keys):
        """
        Returns List<ZNRecord>
        Parameters:
            type: PropertyTypekeys: String


        """
        pass


    def getChildValues(self, clazz, type, keys):
        """
        Returns List<T>
        Parameters:
            clazz: Class<T>type: PropertyTypekeys: String


        Parameterized: <T extends HelixProperty> 
        """
        pass


    def getChildValuesMap(self, clazz, type, keys):
        """
        Returns Map<String, T>
        Parameters:
            clazz: Class<T>type: PropertyTypekeys: String


        Parameterized: <T extends HelixProperty> 
        """
        pass



