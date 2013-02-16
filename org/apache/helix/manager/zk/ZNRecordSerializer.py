# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import ByteArrayInputStream
#from java.io import StringWriter
#from java.util import List
#from java.util import Map
#from org.I0Itec.zkclient.serialize import ZkSerializer
#from org.apache.log4j import Logger
#from org.codehaus.jackson.map import DeserializationConfig
#from org.codehaus.jackson.map import ObjectMapper
#from org.codehaus.jackson.map import SerializationConfig
from org.apache.helix.HelixException import HelixException
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.manager.zk.FakeZkClientInterface import ZkSerializer
from org.apache.helix.util.logger import get_logger
import sys

class ZNRecordSerializer(ZkSerializer):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)


    def getListFieldBound(self, record):
        """
        Returns int
        Parameters:
            record: ZNRecord
        Java modifiers:
             private static

        """
        # int
#        max = Integer.MAX_VALUE
        max = sys.maxint
        if record.getSimpleFields().containsKey(ZNRecord.LIST_FIELD_BOUND):
            # String
            maxStr = record.getSimpleField(ZNRecord.LIST_FIELD_BOUND)
            try:
                max = int(maxStr)
            except ValueError, e:
                self.logger.error("IllegalNumberFormat for list field bound: " + str(maxStr))


        return max


    def serialize(self, data):
        """
        Returns byte[]
        Parameters:
            data: Object
        @Override


        """
        if not (type(data) == ZNRecord): 
            self.logger.error("Input object must be of type ZNRecord but it is " + str(data)+ ". Will not write to zk")
            raise HelixException("Input object is not of type ZNRecord (was " + data + ")")

        return str(data)
        # TODO: fix this
#        # ZNRecord
#        record = (ZNRecord) data
#        # int
#        max = getListFieldBound(record)
#        if max < Integer.MAX_VALUE:
#            # Map<String, List<String>>
#            listMap = record.getListFields()
#            for # String
#            key = None
#             in listMap.keySet()) # List<String>
#                list = listMap.get(key)
#                if list.size() > max:
#                    listMap.put(key, list.subList(0, max))
#
#
#
#        # ObjectMapper
#        mapper = ObjectMapper()
#        # SerializationConfig
#        serializationConfig = mapper.getSerializationConfig()
#        serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, True)
#        serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, True)
#        serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, True)
#        # StringWriter
#        sw = StringWriter()
#        try:
#            mapper.writeValue(sw, data)
#        except Exception, e:
#            logger.error("Exception during data serialization. Will not write to zk. Data (first 1k): " + str(str(sw.toString().substring(0)+ str(1024)))+ str(e))
#            raise HelixException(e)
#
#
#        if sw.toString().getBytes().length > ZNRecord.SIZE_LIMIT:
#            logger.error("Data size larger than 1M, ZNRecord.id: " + str(record.getId())+ ". Will not write to zk. Data (first 1k): " + str(sw.toString().substring(0)+ str(1024)))
#            raise HelixException("Data size larger than 1M, ZNRecord.id: " + record.getId())
#
#
#        return sw.toString().getBytes()


    def deserialize(self, bytes):
        """
        Returns Object
        Parameters:
            bytes: byte[]
        @Override


        """
        if bytes == None or bytes.length == 0: 
            return None

        return bytes
        # TODO: fix this

#        # ObjectMapper
#        mapper = ObjectMapper()
#        # ByteArrayInputStream
#        bais = ByteArrayInputStream(bytes)
#        # DeserializationConfig
#        deserializationConfig = mapper.getDeserializationConfig()
#        deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, True)
#        deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, True)
#        deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, True)
#        try:
#            # ZNRecord
#            zn = mapper.readValue(bais, ZNRecord.class)
#            return zn
#        except Exception, e:
#            logger.error("Exception during deserialization of bytes: " + str(String(bytes))+ str(e))
#            return None




