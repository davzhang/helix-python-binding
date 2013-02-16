# package org.apache.helix.store
#from org.apache.helix.store import *
#from java.io import ByteArrayInputStream
#from java.io import StringWriter
#from org.apache.log4j import Logger
#from org.codehaus.jackson.map import DeserializationConfig
#from org.codehaus.jackson.map import ObjectMapper
#from org.codehaus.jackson.map import SerializationConfig
from org.apache.helix.HelixException import HelixException
from org.apache.helix.ZNRecord import ZNRecord


# Parameterized type: <T>
class PropertyJsonSerializer(PropertySerializer<T>):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(PropertyJsonSerializer.class)



    """

    Parameters:
        Class<T> clazz
    """
    def __init__(self, clazz):
        self._clazz = clazz


    def serialize(self, data):
        """
        Returns byte[]
        Parameters:
            data: T
        @Override


        Throws: 
            PropertyStoreException
        """
        # ObjectMapper
        mapper = ObjectMapper()
        # SerializationConfig
        serializationConfig = mapper.getSerializationConfig()
        serializationConfig.set(SerializationConfig.Feature.INDENT_OUTPUT, True)
        serializationConfig.set(SerializationConfig.Feature.AUTO_DETECT_FIELDS, True)
        serializationConfig.set(SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, True)
        # StringWriter
        sw = StringWriter()
        try:
            mapper.writeValue(sw, data)
            if sw.toString().getBytes().length > ZNRecord.SIZE_LIMIT: 
                raise HelixException("Data size larger than 1M. Write empty string to zk.")


            return sw.toString().getBytes()
        except Exception, e:
            LOG.error("Error during serialization of data (first 1k): " + str(str(sw.toString().substring(0)+ str(1024)))+ str(e))

        return new byte[] {}


    def deserialize(self, bytes):
        """
        Returns T
        Parameters:
            bytes: byte[]
        @Override


        Throws: 
            PropertyStoreException
        """
        # ObjectMapper
        mapper = ObjectMapper()
        # ByteArrayInputStream
        bais = ByteArrayInputStream(bytes)
        # DeserializationConfig
        deserializationConfig = mapper.getDeserializationConfig()
        deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_FIELDS, True)
        deserializationConfig.set(DeserializationConfig.Feature.AUTO_DETECT_SETTERS, True)
        deserializationConfig.set(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, True)
        try:
            # T
            value = mapper.readValue(bais, _clazz)
            return value
        except Exception, e:
            LOG.error("Error during deserialization of bytes: " + str(String(bytes))+ str(e))

        return None



