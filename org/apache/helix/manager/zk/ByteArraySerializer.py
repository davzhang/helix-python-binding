# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from org.I0Itec.zkclient.exception import ZkMarshallingError
#from org.I0Itec.zkclient.serialize import ZkSerializer
from org.apache.helix.manager.zk.FakeZkClientInterface import ZkSerializer


class ByteArraySerializer(ZkSerializer):

    def serialize(self, data):
        """
        Returns byte[]
        Parameters:
            data: Object
        @Override


        Throws: 
            ZkMarshallingError
        """
        return bytes(data)
#        return (byte[]) data


    def deserialize(self, bytes):
        """
        Returns Object
        Parameters:
            bytes: byte[]
        @Override


        Throws: 
            ZkMarshallingError
        """
        return str(bytes)



