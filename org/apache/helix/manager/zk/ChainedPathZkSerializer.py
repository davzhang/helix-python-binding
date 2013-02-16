# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util import ArrayList
#from java.util import Collections
#from java.util import List
#from org.I0Itec.zkclient.exception import ZkMarshallingError
#from org.I0Itec.zkclient.serialize import ZkSerializer
from org.apache.helix.manager.zk.PathBasedZkSerializer import PathBasedZkSerializer
import os

#    class ChainItem(Comparable<ChainItem>):
class ChainItem():

    """

    Parameters:
        String path
        ZkSerializer serializer
    """
    def __init__(self, path, serializer):
        self._path = path
        self._serializer = serializer


    def matches(self, path):
        """
        Returns boolean
        Parameters:
            path: String


        """
        if (self._path == path):
            return True
        else:
            if len(path) > len(self._path):
                if path.startswith(self._path) and path[len(self._path)] == '/':
                    return True
        return False


    def __cmp__(self, o):
        """
        Returns int
        Parameters:
            o: ChainItem
        @Override


        """
        return len(o._path) - len(self._path)


class Builder:

    """
    Java modifiers:
         private
    Parameters:
        ZkSerializer defaultSerializer
    """
    def __init__(self, defaultSerializer):
        self._defaultSerializer = defaultSerializer
        self._items = []


    def serialize(self, path, withSerializer):
        """
        Returns Builder
        Parameters:
            path: StringwithSerializer: ZkSerializer


        """
        self._items.append(ChainItem(os.path.normpath(path), withSerializer))
        return self


    def build(self):
        """
        Returns ChainedPathZkSerializer


        """
        return ChainedPathZkSerializer(self._defaultSerializer, self._items)



class ChainedPathZkSerializer(PathBasedZkSerializer):

    @staticmethod
    def builder(defaultSerializer):
        """
        Returns Builder
        Parameters:
            defaultSerializer: ZkSerializer
        Java modifiers:
             static

        """
        return Builder(defaultSerializer)


    """
    Java modifiers:
         private
    Parameters:
        ZkSerializer defaultSerializer
        List<ChainItem> items
    """
    def __init__(self, defaultSerializer, items):
        self._items = sorted(items)
#        Collections.sort(_items)
        self._defaultSerializer = defaultSerializer


    def serialize(self, data, path):
        """
        Returns byte[]
        Parameters:
            data: Objectpath: String
        @Override


        Throws: 
            ZkMarshallingError
        """
        for item in self._items:
            if item.matches(path):
                return item._serializer.serialize(data)

        return self._defaultSerializer.serialize(data)


    def deserialize(self, bytes, path):
        """
        Returns Object
        Parameters:
            bytes: byte[]path: String
        @Override


        Throws: 
            ZkMarshallingError
        """
        for item in self._items:
            if item.matches(path):
                return item._serializer.deserialize(bytes)

        return self._defaultSerializer.deserialize(bytes)


    def normalize(path):
        """
        Returns String
        Parameters:
            path: String
        Java modifiers:
             private static

        """
        return os.path.normpath()
#        if not path.startsWith("/"):
#            path = "/" + path
#
#        if path.endsWith("/"):
#            path = path.substring(0, path.length() - 1)
#
#        return path



