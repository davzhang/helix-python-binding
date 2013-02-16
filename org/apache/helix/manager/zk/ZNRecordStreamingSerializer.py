# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.io import ByteArrayInputStream
#from java.io import StringWriter
#from java.util import ArrayList
#from java.util import List
#from java.util import Map
#from java.util import TreeMap
#from org.I0Itec.zkclient.exception import ZkMarshallingError
#from org.I0Itec.zkclient.serialize import ZkSerializer
#from org.apache.log4j import Logger
#from org.codehaus.jackson import JsonFactory
#from org.codehaus.jackson import JsonGenerator
#from org.codehaus.jackson import JsonParser
#from org.codehaus.jackson import JsonToken
import io
from org.apache.helix.HelixException import HelixException
from org.apache.helix.ZNRecord import ZNRecord

from org.apache.helix.util.logger import get_logger
import sys, json

#class ZNRecordStreamingSerializer(ZkSerializer):
class ZNRecordStreamingSerializer():

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)

    @staticmethod
    def getListFieldBound(record):
        """
        Returns int
        Parameters:
            record: ZNRecord
        Java modifiers:
             private static

        """
        # int
        max = sys.maxint
        if record.getSimpleFields().__contains__(ZNRecord.LIST_FIELD_BOUND):
            # String
            maxStr = record.getSimpleField(ZNRecord.LIST_FIELD_BOUND)
            try:
                max = int(maxStr)
            except ValueError, e:
                ZNRecordStreamingSerializer.LOG.error("IllegalNumberFormat for list field bound: " + str(maxStr))


        return max


    def serialize(self, data):
        """
        Returns byte[]
        Parameters:
            data: Object
        @Override


        Throws: 
            ZkMarshallingError
        """
        if not (data.__class__ == ZNRecord):
            self.LOG.error("Input object must be of type ZNRecord but it is " + str(data)+ ". Will not write to zk")
            raise HelixException("Input object is not of type ZNRecord (was " + str(data) + ")")


        # ZNRecord
        record = data
        # int
        max = self.getListFieldBound(record)
        if max < sys.maxint:
            # Map<String, List<String>>
            listMap = record.getListFields()
            for key in listMap.keys(): # List<String>
                list = listMap.get(key)
                if list.size() > max: 
                    listMap.__setitem__(key, list[0: max])

        # StringWriter
#        sw = StringWriter()
        outJsonDict = { "id": record.getId(),
                        "simpleFields": record.getSimpleFields(),
                        "listFields": record.getListFields(),
                        "mapFields": record.getMapFields()}
#        sw = io.StringIO()
#        try:
#            # JsonFactory
#            f = JsonFactory()
#            # JsonGenerator
#            g = f.createJsonGenerator(sw)
#            g.writeStartObject()
#            g.writeRaw("\n  ")
#            g.writeStringField("id", record.getId())
#            g.writeRaw("\n  ")
#            g.writeObjectFieldStart("simpleFields")
#            for # String
#            key = None
#             in record.getSimpleFields().keySet()) g.writeRaw("\n    ")
#                g.writeStringField(key, record.getSimpleField(key))
#
#            g.writeRaw("\n  ")
#            g.writeEndObject()
#            g.writeRaw("\n  ")
#            g.writeObjectFieldStart("listFields")
#            for # String
#            key = None
#             in record.getListFields().keySet()) g.writeRaw("\n    ")
#                g.writeArrayFieldStart(key)
#                # List<String>
#                list = record.getListField(key)
#                for # String
#                listValue = None
#                 in list) g.writeString(listValue)
#
#                g.writeEndArray()
#
#            g.writeRaw("\n  ")
#            g.writeEndObject()
#            g.writeRaw("\n  ")
#            g.writeObjectFieldStart("mapFields")
#            for # String
#            key = None
#             in record.getMapFields().keySet()) g.writeRaw("\n    ")
#                g.writeObjectFieldStart(key)
#                # Map<String, String>
#                map = record.getMapField(key)
#                for # String
#                mapKey = None
#                 in map.keySet()) g.writeRaw("\n      ")
#                    g.writeStringField(mapKey, map.get(mapKey))
#
#                g.writeRaw("\n    ")
#                g.writeEndObject()
#
#            g.writeRaw("\n  ")
#            g.writeEndObject()
#            g.writeRaw("\n")
#            g.writeEndObject()
#            g.close()
#        except Exception, e:
#            LOG.error("Exception during data serialization. Will not write to zk. Data (first 1k): " + str(str(sw.toString().substring(0)+ str(1024)))+ str(e))
#            raise HelixException(e)
#
#
#        if sw.toString().getBytes().length > ZNRecord.SIZE_LIMIT:
#            LOG.error("Data size larger than 1M, ZNRecord.id: " + str(record.getId())+ ". Will not write to zk. Data (first 1k): " + str(sw.toString().substring(0)+ str(1024)))
#            raise HelixException("Data size larger than 1M, ZNRecord.id: " + record.getId())


#        return sw.toString().getBytes()
        return bytes(str(json.dumps(outJsonDict,sort_keys=True)))   # put id first


    def deserialize(self, bytes):
        """
        Returns Object
        Parameters:
            bytes: byte[]
        @Override


        Throws: 
            ZkMarshallingError
        """
        if bytes == None or len(bytes) == 0:
            self.LOG.error("ZNode is empty.")
            return None

        # ByteArrayInputStream
        inputJson = json.loads(str(bytes))
        # ZNRecord
        record = ZNRecord(str(inputJson["id"]))
        record.setSimpleFields(inputJson["simpleFields"])
        record.setListFields(inputJson["listFields"])
        record.setMapFields(inputJson["mapFields"])
#        record = None
#        try:
#            # JsonFactory
#            f = JsonFactory()
#            # JsonParser
#            jp = f.createJsonParser(bais)
#            jp.nextToken()
#            while (jp.nextToken() != JsonToken.END_OBJECT:
#                # String
#                fieldname = jp.getCurrentName()
#                jp.nextToken()
#                if ("id" == fieldname):
#                    record = ZNRecord(jp.getText())
#                else:
#                    if ("simpleFields" == fieldname):
#                        while (jp.nextToken() != JsonToken.END_OBJECT:
#                            # String
#                            key = jp.getCurrentName()
#                            jp.nextToken()
#                            record.setSimpleField(key, jp.getText())
#
#                    else:
#                        if ("mapFields" == fieldname):
#                            while (jp.nextToken() != JsonToken.END_OBJECT:
#                                # String
#                                key = jp.getCurrentName()
#                                record.setMapField(key, TreeMap<String, String>())
#                                jp.nextToken()
#                                while (jp.nextToken() != JsonToken.END_OBJECT:
#                                    # String
#                                    mapKey = jp.getCurrentName()
#                                    jp.nextToken()
#                                    record.getMapField(key).put(mapKey, jp.getText())
#
#
#                        else:
#                            if ("listFields" == fieldname):
#                                while (jp.nextToken() != JsonToken.END_OBJECT:
#                                    # String
#                                    key = jp.getCurrentName()
#                                    record.setListField(key, ArrayList<String>())
#                                    jp.nextToken()
#                                    while (jp.nextToken() != JsonToken.END_ARRAY:
#                                        record.getListField(key).add(jp.getText())
#
#
#                            else:
#                                raise IllegalStateException("Unrecognized field '" + fieldname + "'!")
#
#
#
#            jp.close()
#        except Exception, e:
#            LOG.error("Exception during deserialization of bytes: " + str(String(bytes))+ str(e))

        return record


#    def main(args):
#        """
#        Returns void
#        Parameters:
#            args: String[]
#        Java modifiers:
#             static
#
#        """
#        # ZNRecord
#        record = ZNRecord("record")
#        # int
#        recordSize = 10
#        for (# int
#        i = 0; i < recordSize; i++) record.setSimpleField("" + i, "" + i)
#            record.setListField("" + i, ArrayList<String>())
#            for (# int
#            j = 0; j < recordSize; j++) record.getListField("" + i).add("" + j)
#
#            record.setMapField("" + i, TreeMap<String, String>())
#            for (# int
#            j = 0; j < recordSize; j++) record.getMapField("" + i).put("" + j, "" + j)
#
#
#        # ZNRecordStreamingSerializer
#        serializer = ZNRecordStreamingSerializer()
#        # byte[]
#        bytes = serializer.serialize(record)
#        System.out.println(String(bytes))
#        # ZNRecord
#        record2 = (ZNRecord) serializer.deserialize(bytes)
#        System.out.println(record2)
#        # long
#        start = System.currentTimeMillis()
#        for (# int
#        i = 0; i < 100; i++) bytes = serializer.serialize(record)
#            record2 = (ZNRecord) serializer.deserialize(bytes)
#
#        # long
#        end = System.currentTimeMillis()
#        System.out.println("ZNRecordStreamingSerializer time used: " + (end - start))
#        # ZNRecordSerializer
#        serializer2 = ZNRecordSerializer()
#        bytes = serializer2.serialize(record)
#        record2 = (ZNRecord) serializer2.deserialize(bytes)
#        start = System.currentTimeMillis()
#        for (# int
#        i = 0; i < 100; i++) bytes = serializer2.serialize(record)
#            record2 = (ZNRecord) serializer2.deserialize(bytes)
#
#        end = System.currentTimeMillis()
#        System.out.println("ZNRecordSerializer time used: " + (end - start))
#
#
#
