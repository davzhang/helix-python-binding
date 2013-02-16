# package org.apache.helix
#from org.apache.helix import *
#from java.util import HashMap
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.ZNRecord import ZNRecord

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.UserExceptions import IllegalArgumentException

class ZNRecordBucketizer:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)


    """

    Parameters:
        int bucketSize
    """
    def __init__(self, bucketSize):
        if bucketSize <= 0: 
            self.LOG.debug("bucketSize <= 0 (was " + str(bucketSize)+ "). Set to 0 to use non-bucketized HelixProperty.")
            bucketSize = 0

        self._bucketSize = bucketSize


    def getBucketName(self, key):
        """
        Returns String
        Parameters:
            key: String


        """
        if self._bucketSize == 0: 
            return None

        # int
        idx = key.lastIndexOf('_')
        if idx < 0: 
            raise IllegalArgumentException("Could NOT find partition# in " + key + ". partitionName should be in format of resourceName_partition#")


        try:
            # int
            partitionNb = int(key.substring(idx + 1))
            # int
            bucketNb = partitionNb / self._bucketSize
            # int
            startPartition = bucketNb * self._bucketSize
            # int
            endPartition = bucketNb * self._bucketSize + (self._bucketSize - 1)
            return key.substring(0, idx) + "_p" + startPartition + "-p" + endPartition
        except ValueError, e:
            raise IllegalArgumentException("Could NOT parse partition# (" + key.substring(idx + 1) + ") in " + key)




    def bucketize(self, record):
        """
        Returns Map<String, ZNRecord>
        Parameters:
            record: ZNRecord


        """
        # Map<String, ZNRecord>
        map = {}
        if self._bucketSize == 0: 
            map.put(record.getId(), record)
            return map

        for partitionName in record.getListFields().keys(): # String
            bucketName = self.getBucketName(partitionName)
            if bucketName != None: 
                if not map.__contains__(bucketName):
                    map.__setitem__(bucketName, ZNRecord(bucketName))

                # ZNRecord
                bucketizedRecord = map.get(bucketName)
                bucketizedRecord.setListField(partitionName, record.getListField(partitionName))
            else:
                self.LOG.error("Can't bucketize " + str(partitionName)+ " in list field")


        for partitionName in record.getMapFields().keys(): # String
            bucketName = self.getBucketName(partitionName)
            if bucketName != None: 
                if not map.__contains__(bucketName):
                    map.__setitem__(bucketName, ZNRecord(bucketName))

                # ZNRecord
                bucketizedRecord = map.get(bucketName)
                bucketizedRecord.setMapField(partitionName, record.getMapField(partitionName))
            else:
                self.LOG.error("Can't bucketize " + str(partitionName)+ " in map field")


        for bucketizedRecord in map.values(): bucketizedRecord.setSimpleFields(record.getSimpleFields())

        return map



