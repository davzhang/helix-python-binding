# package org.apache.helix.model
#from org.apache.helix.model import *

class Partition:



    def getPartitionName(self):
        """
        Returns String


        """
        return _partitionName


    """

    Parameters:
        String partitionName
    """
    def __init__(self, partitionName):
        self._partitionName = partitionName


    def equals(self, obj):
        """
        Returns boolean
        Parameters:
            obj: Object
        @Override


        """
        if obj == None or not (type(obj) == Partition): 
            return False

        # Partition
        that = (Partition) obj
        return (_partitionName == that.getPartitionName())


    def hashCode(self):
        """
        Returns int
        @Override


        """
        return _partitionName.hashCode()


    def toString(self):
        """
        Returns String
        @Override


        """
        return _partitionName



