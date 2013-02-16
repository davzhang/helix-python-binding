# package org.apache.helix.store.zk
#from org.apache.helix.store.zk import *
#from java.util import Collections
#from java.util import HashSet
#from java.util import List
#from java.util import Set
#from org.apache.zookeeper.data import Stat
import copy
from kazoo.protocol.states import ZnodeStat
from org.apache.helix.util.ZKConstants import HelixZNodeStat


class ZNode:

    """
    Java modifiers:
         final static
    Type:
        Stat
    """
    ZERO_STAT = HelixZNodeStat()


    """

    Parameters:
        String zkPath
        Object data
        Stat stat
    """
    def __init__(self, zkPath, data, stat):
        self._zkPath = zkPath
#        self._childSet = Collections.emptySet()
        self._childSet = set()
        self._data = data
        self._stat = stat


    def removeChild(self, child):
        """
        Returns void
        Parameters:
            child: String


        """
#        if self._childSet != Collections.emptySet():
        if self._childSet:
            self._childSet.remove(child)



    def addChild(self, child):
        """
        Returns void
        Parameters:
            child: String


        """
        if not self._childSet:
            self._childSet = set()

        self._childSet.add(child)


    def addChildren(self, children):
        """
        Returns void
        Parameters:
            children: List<String>


        """
#        if children != None and not children.isEmpty():
        if children:
            if not self._childSet:
                self._childSet = set()

            self._childSet.update(children)


    def hasChild(self, child):
        """
        Returns boolean
        Parameters:
            child: String


        """
        return self._childSet.__contains__(child)


    def getChildSet(self):
        """
        Returns Set<String>


        """
        return self._childSet


    def setData(self, data):
        """
        Returns void
        Parameters:
            data: Object


        """
        self._data = data


    def getData(self):
        """
        Returns Object


        """
        return self._data


    def setStat(self, stat):
        """
        Returns void
        Parameters:
            stat: Stat


        """
        self._stat = stat


    def getStat(self):
        """
        Returns Stat


        """
        return self._stat


    def setChildSet(self, childNames):
        """
        Returns void
        Parameters:
            childNames: List<String>


        """
#            if self._childSet == Collections.emptySet():
        if childNames:
#            if not self._childSet:
#                self._childSet = set()
            self._childSet = copy.copy(childNames)

#            self._childSet.clear()
#            self._childSet.addAll(childNames)



    def toString(self):
        """
        Returns String
        @Override


        """
        return self._zkPath + ", " + self._data + ", " + self._childSet + ", " + self._stat



