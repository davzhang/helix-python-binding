# package org.apache.helix
#from org.apache.helix import *
#from java.util import List
#from org.I0Itec.zkclient import DataUpdater
#from org.I0Itec.zkclient import IZkChildListener
#from org.I0Itec.zkclient import IZkDataListener
#from org.apache.zookeeper.data import Stat


# Parameterized type: <T>
class BaseDataAccessor:

    def create(self, path, record, options):
        """
        Returns boolean
        Parameters:
            path: Stringrecord: Toptions: int


        """
        pass


    def set(self, path, record, options):
        """
        Returns boolean
        Parameters:
            path: Stringrecord: Toptions: int


        """
        pass


    def update(self, path, updater, options):
        """
        Returns boolean
        Parameters:
            path: Stringupdater: DataUpdater<T>options: int


        """
        pass


    def remove(self, path, options):
        """
        Returns boolean
        Parameters:
            path: Stringoptions: int


        """
        pass


    def createChildren(self, paths, records, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>records: List<T>options: int


        """
        pass


    def setChildren(self, paths, records, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>records: List<T>options: int


        """
        pass


    def updateChildren(self, paths, updaters, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>updaters: List<DataUpdater<T>>options: int


        """
        pass


    def remove(self, paths, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>options: int


        """
        pass


    def get(self, path, stat, options):
        """
        Returns T
        Parameters:
            path: Stringstat: Statoptions: int


        """
        pass


    def get(self, paths, stats, options):
        """
        Returns List<T>
        Parameters:
            paths: List<String>stats: List<Stat>options: int


        """
        pass


    def getChildren(self, parentPath, stats, options):
        """
        Returns List<T>
        Parameters:
            parentPath: Stringstats: List<Stat>options: int


        """
        pass


    def getChildNames(self, parentPath, options):
        """
        Returns List<String>
        Parameters:
            parentPath: Stringoptions: int


        """
        pass


    def exists(self, path, options):
        """
        Returns boolean
        Parameters:
            path: Stringoptions: int


        """
        pass


    def exists(self, paths, options):
        """
        Returns boolean[]
        Parameters:
            paths: List<String>options: int


        """
        pass


    def getStats(self, paths, options):
        """
        Returns Stat[]
        Parameters:
            paths: List<String>options: int


        """
        pass


    def getStat(self, path, options):
        """
        Returns Stat
        Parameters:
            path: Stringoptions: int


        """
        pass


    def subscribeDataChanges(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: IZkDataListener


        """
        pass


    def unsubscribeDataChanges(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: IZkDataListener


        """
        pass


    def subscribeChildChanges(self, path, listener):
        """
        Returns List<String>
        Parameters:
            path: Stringlistener: IZkChildListener


        """
        pass


    def unsubscribeChildChanges(self, path, listener):
        """
        Returns void
        Parameters:
            path: Stringlistener: IZkChildListener


        """
        pass


    def reset(self):
        """
        Returns void


        """
        pass



