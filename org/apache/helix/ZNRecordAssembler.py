# package org.apache.helix
#from org.apache.helix import *
#from java.util import List
from org.apache.helix.ZNRecord import ZNRecord


class ZNRecordAssembler:

    def assemble(self, records):
        """
        Returns ZNRecord
        Parameters:
            records: List<ZNRecord>


        """
        # ZNRecord
        assembledRecord = None
        if records != None and records.size() > 0:
            for record  in records:
                if record == None:
                    continue

                if assembledRecord == None: 
                    assembledRecord = ZNRecord(record.getId())

                assembledRecord.merge(record)


        return assembledRecord



