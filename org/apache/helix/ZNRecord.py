# package org.apache.helix
#from org.apache.helix import *
#from java.util import ArrayList
#from java.util import List
#from java.util import Map
#from java.util import TreeMap
#from org.apache.log4j import Logger
#from org.codehaus.jackson.annotate import JsonCreator
#from org.codehaus.jackson.annotate import JsonIgnore
#from org.codehaus.jackson.annotate import JsonIgnoreProperties
#from org.codehaus.jackson.annotate import JsonProperty
#from org.apache.helix.ZNRecordDelta import MergeOperation
import copy
import logging
from org.apache.helix.ZNRecordDelta import ZNRecordDelta
from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.logger import get_logger

from org.apache.helix.util.misc import enum

MergeOperation  = enum(
    'ADD', 'SUBTRACT'
)

# Annotation @JsonIgnoreProperties(ignoreUnknown = True)


class ZNRecord:

    """
    Java modifiers:
         static
    Type:
        Logger
    """
#    _logger = Logger.getLogger(__name__)
    _logger = get_logger(__name__)
#    console = logging.StreamHandler()
#    formatter = logging.Formatter('%(asctime)-15s [%(process)d] [%(levelname)s] %(message)s')
#    console.setFormatter(formatter)
#    console.setLevel(logging.INFO)
#    _logger.addHandler(console)
    _logger.setLevel(logging.INFO)

    """
    # Annotation: @JsonIgnore(True)

    Java modifiers:
         final static
    Type:
        String
    """
    LIST_FIELD_BOUND = "listField.bound"

    """
    # Annotation: @JsonIgnore(True)

    Java modifiers:
         final static
    Type:
        int
    """
    SIZE_LIMIT = 1000 * 1024

    def __init__(self, *args):
        if len(args) == 1 and (isinstance(args[0], str) or isinstance(args[0],unicode)):
            self.__init_id__(args[0])
        elif len(args) == 1 and isinstance(args[0], ZNRecord):
            self.__init_record__(args[0])
        elif len(args) == 2 and isinstance(args[0],ZNRecord) and (isinstance(args[1],str) or isinstance(args[1], unicode)):
            self.__init_record_id__(self, *args)
        elif len(args) == 2 and isinstance(args[0],ZNRecord) and isinstance(args[1],int):
            self.__init_record_version__(self, *args)
        else:
            raise IllegalArgumentException("Input arguments not supported. args = %s" % args)

    """
    @JsonCreator

    Parameters:
        String id
    """
    def __init_id__(self, id):
        self.id = id
#        simpleFields = TreeMap<String, String>()
#        mapFields = TreeMap<String, Map<String, String>>()
#        listFields = TreeMap<String, List<String>>()
        self.simpleFields = {}
        self.mapFields = {}
        self.listFields = {}
        self._version = 0  # TODO: What should be the default?
        self._creationTime = None
        self._modifiedTime = None
        self._deltaList = []


    """

    Parameters:
        ZNRecord record
    """
    def __init_record__(self, record):
        self.__init_record_id__(record, record.getId())


    """

    Parameters:
        ZNRecord record
        String id
    """
    def __init_record_id__(self, record, id):
        self.__init_id__(id)
        self.simpleFields= copy.deepcopy(record.getSimpleFields())
        self.mapFields= copy.deepcopy(record.getMapFields())
        self.listFields = copy.deepcopy(record.getListFields())
        self._version = record.getVersion()
        self._creationTime = record.getCreationTime()
        self._modifiedTime = record.getModifiedTime()


    """

    Parameters:
        ZNRecord record
        int version
    """
    def __init_record_version__(self, record, version):
        self.__init__(record)
        self._version = version


    def setDeltaList(self, deltaList):
        """
        Returns void
        Parameters:
            deltaList: List<ZNRecordDelta>
        # Annotation: @JsonIgnore(True)



        """
        self._deltaList = deltaList


    def getDeltaList(self):
        """
        Returns List<ZNRecordDelta>
        # Annotation: @JsonIgnore(True)



        """
        return self._deltaList


    def getSimpleFields(self):
        """
        Returns Map<String, String>
        @JsonProperty


        """
        return self.simpleFields


    def setSimpleFields(self, simpleFields):
        """
        Returns void
        Parameters:
            simpleFields: Map<String, String>
        @JsonProperty


        """
        self.simpleFields = simpleFields


    def getMapFields(self):
        """
        Returns Map<String, Map<String, String>>
        @JsonProperty


        """
        return self.mapFields


    def setMapFields(self, mapFields):
        """
        Returns void
        Parameters:
            mapFields: Map<String, Map<String, String>>
        @JsonProperty


        """
        self.mapFields = mapFields


    def getListFields(self):
        """
        Returns Map<String, List<String>>
        @JsonProperty


        """
        return self.listFields


    def setListFields(self, listFields):
        """
        Returns void
        Parameters:
            listFields: Map<String, List<String>>
        @JsonProperty


        """
        self.listFields = listFields


    def setSimpleField(self, k, v):
        """
        Returns void
        Parameters:
            k: Stringv: String
        @JsonProperty


        """
        self.simpleFields.__setitem__(k, v)


    def getId(self):
        """
        Returns String
        @JsonProperty


        """
        return self.id


    def setMapField(self, k, v):
        """
        Returns void
        Parameters:
            k: Stringv: Map<String, String>


        """
        self.mapFields.__setitem__(k, v)


    def setListField(self, k, v):
        """
        Returns void
        Parameters:
            k: Stringv: List<String>


        """
        self.listFields.__setitem__(k, v)


    def getSimpleField(self, k):
        """
        Returns String
        Parameters:
            k: String


        """
#        return str(self.simpleFields.get(k))    # get rid of the unicode, this will cause the None to return as 'None'
        return self.simpleFields.get(k)    # get rid of the unicode


    def getMapField(self, k):
        """
        Returns Map<String, String>
        Parameters:
            k: String


        """
        return self.mapFields.get(k)


    def getListField(self, k):
        """
        Returns List<String>
        Parameters:
            k: String


        """
        return self.listFields.get(k)


    def __str__(self):
        """
        Returns String
        @Override


        """
        # StringBuffer
        return self.id + ", " + str(self.simpleFields) + str(self.mapFields) + str(self.listFields)
#        sb = []
#        sb.append(id + ", ")
#        if self.simpleFields != None:
#            sb.append(simpleFields)
#
#        if mapFields != None:
#            sb.append(mapFields)
#
#        if listFields != None:
#            sb.append(listFields)
#
#        return sb.toString()


    def merge_record(self, record):
        """
        Returns void
        Parameters:
            record: ZNRecord


        """
        if record == None: 
            return

        if len(record.getDeltaList()) > 0:
            self._logger.info("Merging with delta list, recordId = " + self.id + " other:" + record.getId())
            self.merge(record.getDeltaList())
            return

        #simpleFields.putAll(record.simpleFields)
        self.simpleFields.update(record.simpleFields)
        for key in record.mapFields.keys():
            map = record.mapFields.get(key)
            if map != None:
                map.update(record.mapFields[key])
#                map.putAll(record.mapFields.get(key))
            else:
                self.mapFields.__setitem__(key, record.mapFields.get(key))


        for key in record.listFields.keys(): # List<String>
            list = self.listFields.get(key)
            if list != None: 
                list.extend(record.listFields.get(key))
            else:
                self.listFields.__setitem__(key, record.listFields.get(key))



    def merge_delta(self, delta):
        """
        Returns void
        Parameters:
            delta: ZNRecordDelta


        """
        if delta.getMergeOperation() == MergeOperation.ADD: 
            self.merge(delta.getRecord())
        else:
            if delta.getMergeOperation() == MergeOperation.SUBTRACT: 
                self.subtract(delta.getRecord())



    def merge_delta_list(self, deltaList):
        """
        Returns void
        Parameters:
            deltaList: List<ZNRecordDelta>


        """
        for delta in deltaList:
          self.merge(delta)



    def equals(self, obj):
        """
        Returns boolean
        Parameters:
            obj: Object
        @Override


        """
        if not (type(obj) == ZNRecord): 
            return False

        # ZNRecord
        that = obj
#        that = (ZNRecord) obj
        if self.getSimpleFields().size() != that.getSimpleFields().size():
            return False

        if self.getMapFields().size() != that.getMapFields().size(): 
            return False

        if self.getListFields().size() != that.getListFields().size(): 
            return False

        if not (self.getSimpleFields() == that.getSimpleFields()):
            return False

        if not (self.getMapFields() == that.getMapFields()):
            return False

        if not (self.getListFields() == that.getListFields()):
            return False

        return True


    def subtract(self, value):
        """
        Returns void
        Parameters:
            value: ZNRecord


        """
        for key in value.getSimpleFields().keys():
            if key in self.simpleFields:
                self.simpleFields.pop(key)


        for key in value.getListFields().keys():
            if key in self.listFields.containsKey:
                self.listFields.pop(key)


        for key in value.getMapFields().keys():
            if key in self.mapFields:
                self.mapFields.pop(key)




    def getVersion(self):
        """
        Returns int
        # Annotation: @JsonIgnore(True)



        """
        return self._version


    def setVersion(self, version):
        """
        Returns void
        Parameters:
            version: int
        # Annotation: @JsonIgnore(True)



        """
        self._version = version


    def getCreationTime(self):
        """
        Returns long
        # Annotation: @JsonIgnore(True)



        """
        return self._creationTime


    def setCreationTime(self, creationTime):
        """
        Returns void
        Parameters:
            creationTime: long
        # Annotation: @JsonIgnore(True)



        """
        _creationTime = creationTime


    def getModifiedTime(self):
        """
        Returns long
        # Annotation: @JsonIgnore(True)



        """
        return self._modifiedTime


    def setModifiedTime(self, modifiedTime):
        """
        Returns void
        Parameters:
            modifiedTime: long
        # Annotation: @JsonIgnore(True)



        """
        _modifiedTime = modifiedTime


    def merge(self, *args):
        if len(args) == 1 and isinstance(args[0], ZNRecord):
            self.merge_record(*args)
        elif len(args) == 1 and isinstance(args[0], ZNRecordDelta):
            self.merge_delta(*args)
        elif len(args) == 1 and isinstance(args[0], list):
            self.merge_delta_list(*args)
        else:
            raise IllegalArgumentException("Input arguments not supported. args = %s" % args)


