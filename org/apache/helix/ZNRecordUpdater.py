# package org.apache.helix
#from org.apache.helix import *
#from org.I0Itec.zkclient import DataUpdater
from org.apache.helix.manager.zk.FakeZkClientInterface import DataUpdater


class ZNRecordUpdater(DataUpdater):
#    class ZNRecordUpdater(DataUpdater<ZNRecord>):



    """

    Parameters:
        ZNRecord record
    """
    def __init__(self, record):
        self._record = record


    def update(self, current):
        """
        Returns ZNRecord
        Parameters:
            current: ZNRecord
        @Override


        """
        if current != None: 
            current.merge(self._record)
            return current

        return self._record



