# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from org.I0Itec.zkclient.serialize import ZkSerializer
from org.apache.helix.manager.zk.PathBasedZkSerializer import PathBasedZkSerializer


class BasicZkSerializer(PathBasedZkSerializer):



    """

    Parameters:
        ZkSerializer delegate
    """
    def __init__(self, delegate):
        self._delegate = delegate


    def serialize(self, data, path):
        """
        Returns byte[]
        Parameters:
            data: Objectpath: String


        """
        return self._delegate.serialize(data)


    def deserialize(self, bytes, path):
        """
        Returns Object
        Parameters:
            bytes: byte[]path: String
        @Override


        """
        return self._delegate.deserialize(bytes)



