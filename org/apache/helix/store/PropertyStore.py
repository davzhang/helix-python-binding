# package org.apache.helix.store
#from org.apache.helix.store import *
#from java.util import Comparator
#from java.util import List
#from org.I0Itec.zkclient import DataUpdater


# Parameterized type: <T>
class PropertyStore:

    def setProperty(self, key, value):
        """
        Returns void
        Parameters:
            key: Stringvalue: T


        Throws: 
            PropertyStoreException
        """
        pass


    def getProperty(self, key):
        """
        Returns T
        Parameters:
            key: String


        Throws: 
            PropertyStoreException
        """
        pass


    def getProperty(self, key, propertyStat):
        """
        Returns T
        Parameters:
            key: StringpropertyStat: PropertyStat


        Throws: 
            PropertyStoreException
        """
        pass


    def removeProperty(self, key):
        """
        Returns void
        Parameters:
            key: String


        Throws: 
            PropertyStoreException
        """
        pass


    def getPropertyNames(self, prefix):
        """
        Returns List<String>
        Parameters:
            prefix: String


        Throws: 
            PropertyStoreException
        """
        pass


    def setPropertyDelimiter(self, delimiter):
        """
        Returns void
        Parameters:
            delimiter: String


        Throws: 
            PropertyStoreException
        """
        pass


    def subscribeForPropertyChange(self, prefix, listener):
        """
        Returns void
        Parameters:
            prefix: Stringlistener: PropertyChangeListener<T>


        Throws: 
            PropertyStoreException
        """
        pass


    def unsubscribeForPropertyChange(self, prefix, listener):
        """
        Returns void
        Parameters:
            prefix: Stringlistener: PropertyChangeListener<T>


        Throws: 
            PropertyStoreException
        """
        pass


    def canParentStoreData(self):
        """
        Returns boolean


        """
        pass


    def setPropertySerializer(self, serializer):
        """
        Returns void
        Parameters:
            serializer: PropertySerializer<T>


        """
        pass


    def createPropertyNamespace(self, prefix):
        """
        Returns void
        Parameters:
            prefix: String


        Throws: 
            PropertyStoreException
        """
        pass


    def getPropertyRootNamespace(self):
        """
        Returns String


        """
        pass


    def updatePropertyUntilSucceed(self, key, updater):
        """
        Returns void
        Parameters:
            key: Stringupdater: DataUpdater<T>


        Throws: 
            PropertyStoreException
        """
        pass


    def updatePropertyUntilSucceed(self, key, updater, createIfAbsent):
        """
        Returns void
        Parameters:
            key: Stringupdater: DataUpdater<T>createIfAbsent: boolean


        Throws: 
            PropertyStoreException
        """
        pass


    def exists(self, key):
        """
        Returns boolean
        Parameters:
            key: String


        """
        pass


    def removeNamespace(self, prefix):
        """
        Returns void
        Parameters:
            prefix: String


        Throws: 
            PropertyStoreException
        """
        pass


    def compareAndSet(self, key, expected, update, comparator):
        """
        Returns boolean
        Parameters:
            key: Stringexpected: Tupdate: Tcomparator: Comparator<T>


        """
        pass


    def compareAndSet(self, key, expected, update, comparator, createIfAbsent):
        """
        Returns boolean
        Parameters:
            key: Stringexpected: Tupdate: Tcomparator: Comparator<T>createIfAbsent: boolean


        """
        pass


    def start(self):
        """
        Returns boolean


        """
        pass


    def stop(self):
        """
        Returns boolean


        """
        pass



