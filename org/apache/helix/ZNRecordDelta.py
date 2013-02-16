# package org.apache.helix
#from org.apache.helix import *
#from org.apache.helix.ZNRecord import ZNRecord
import copy

from org.apache.helix.util.misc import enum

MergeOperation=enum('ADD', 'SUBTRACT')

class ZNRecordDelta:

    """

    Parameters:
        ZNRecord record
        MergeOperation _mergeOperation
    """
    def __init__(self, record, mergeOperation = MergeOperation.ADD):
#        self._record = ZNRecord(record)
        self._record = copy.deepcopy(record)
        self._mergeOperation = mergeOperation

    def getRecord(self):
        """
        Returns ZNRecord


        """
        return self._record


    def getMergeOperation(self):
        """
        Returns MergeOperation


        """
        return self._mergeOperation



