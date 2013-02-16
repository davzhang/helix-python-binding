# package org.apache.helix
#from org.apache.helix import *
#from java.lang.reflect import Constructor
#from java.util import ArrayList
#from java.util import Collection
#from java.util import Collections
#from java.util import HashMap
#from java.util import List
#from java.util import Map

from org.apache.helix.util.misc import enum
from org.apache.helix.ZNRecord import ZNRecord
import traceback

HelixPropertyAttribute = enum('BUCKET_SIZE', 'GROUP_MESSAGE_MODE')

class HelixProperty(object):

    def __init__(self, *args):
        self._record = ZNRecord(*args)

#    """
#
#    Parameters:
#        String id
#    """
#    def __init__(self, id):
#        self._record = ZNRecord(id)
#
#
#    """
#
#    Parameters:
#        ZNRecord record
#    """
#    def __init__(self, record):
#        self._record = ZNRecord(record)


    def getId(self):
        """
        Returns String
        Java modifiers:
             final

        """
        return self._record.getId()


    def getRecord(self):
        """
        Returns ZNRecord
        Java modifiers:
             final

        """
        return self._record


    def setDeltaList(self, deltaList):
        """
        Returns void
        Parameters:
            deltaList: List<ZNRecordDelta>
        Java modifiers:
             final

        """
        self._record.setDeltaList(deltaList)


    def toString(self):
        """
        Returns String
        @Override


        """
        return self._record.toString()


    def getBucketSize(self):
        """
        Returns int


        """
        # String
        bucketSizeStr = self._record.getSimpleField('BUCKET_SIZE')
        # int
        bucketSize = 0
        if bucketSizeStr != None: 
            try:
                bucketSize = int(bucketSizeStr)
            except ValueError, e: pass


        return bucketSize


    def setBucketSize(self, bucketSize):
        """
        Returns void
        Parameters:
            bucketSize: int


        """
        if bucketSize <= 0: 
            bucketSize = 0
        self._record.setSimpleField('BUCKET_SIZE', "" + str(bucketSize))

    @staticmethod
    def convertToTypedInstance(clazz, record):
        """
        Returns T
        Parameters:
            clazz: Class<T>record: ZNRecord
        Java modifiers:
             static

        Parameterized: <T extends HelixProperty>
        """
        if record == None:
            return None

        try:
            # Constructor<T>
#            getConstructor = clazz.getConstructor(new Class[] { ZNRecord.class })
#            constructor = clazz(re)
#            return constructor.newInstance(record)
#            return clazz(record)
#            return clazz.getTypeClass()(record)    # call constructor
#            return type(clazz)(record)    # call constructor
            return clazz(record)    # call constructor
        except :
            print traceback.format_exc()

        return None


    @staticmethod
    def convertToTypedList(clazz, records):
        """
        Returns List<T>
        Parameters:
            clazz: Class<T>records: Collection<ZNRecord>
        Java modifiers:
             static

        Parameterized: <T extends HelixProperty>
        """
        if records == None:
            return None

        # List<T>
        decorators = []
        for record in records: # T
            decorator = HelixProperty.convertToTypedInstance(clazz, record)
            if decorator != None:
                decorators.add(decorator)


        return decorators


    @staticmethod
    def convertListToMap(records):
        """
        Returns Map<String, T>
        Parameters:
            records: List<T>
        Java modifiers:
             static

        Parameterized: <T extends HelixProperty>
        """
        if records == None:
            return {}

        # Map<String, T>
        decorators = {}
        for record in records: decorators.__setitem__(record.getId(), record)

        return decorators


    @staticmethod
    def convertToList(typedInstances):
        """
        Returns List<ZNRecord>
        Parameters:
            typedInstances: List<T>
        Java modifiers:
             static

        Parameterized: <T extends HelixProperty>
        """
        if typedInstances == None:
            return []

        # List<ZNRecord>
        records = []
        for typedInstance in typedInstances: records.append(typedInstance.getRecord())

        return records


    def setGroupMessageMode(self, enable):
        """
        Returns void
        Parameters:
            enable: boolean


        """
        self._record.setSimpleField('GROUP_MESSAGE_MODE', "" + str(enable))


    def getGroupMessageMode(self):
        """
        Returns boolean


        """
        # String
        enableStr = self._record.getSimpleField('GROUP_MESSAGE_MODE')
        if enableStr == None:
            return False
        try:
          groupMode = eval(enableStr.lower().capitalize())
        except: return False
        if not groupMode: return False
        return groupMode



    def isValid(self):
        """
        Returns boolean


        """
        return False


    def __eq__(self, obj):
        """
        Returns boolean
        Parameters:
            obj: Object
        @Override


        """
        if obj == None: 
            return False

        if type(obj) == HelixProperty: 
            # HelixProperty
            that = obj
            if that.getRecord() != None: 
                return (that.getRecord() == self.getRecord())


        return False



