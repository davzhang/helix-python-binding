# package org.apache.helix.store
#from org.apache.helix.store import *
#from org.apache.log4j import Logger
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.manager.zk.ZNRecordSerializer import ZNRecordSerializer
from org.apache.helix.store.PropertySerializer import PropertySerializer

from org.apache.helix.util.logger import get_logger


class ZNRecordJsonSerializer(PropertySerializer):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)



    def serialize(self, data):
        """
        Returns byte[]
        Parameters:
            data: ZNRecord
        @Override


        Throws: 
            PropertyStoreException
        """
        return self._serializer.serialize(data)


    def deserialize(self, bytes):
        """
        Returns ZNRecord
        Parameters:
            bytes: byte[]
        @Override


        Throws: 
            PropertyStoreException
        """
#        return (ZNRecord) self._serializer.deserialize(bytes)
        return self._serializer.deserialize(bytes)



