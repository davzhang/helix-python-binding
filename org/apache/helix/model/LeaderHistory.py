# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import ArrayList
#from java.util import List
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.HelixProperty import HelixProperty


class LeaderHistory(HelixProperty):

    """
    Java modifiers:
         private final static
    Type:
        int
    """
    HISTORY_SIZE = 8

    """

    Parameters:
        String id
    """
    def __init__(self, id):
        super(id)


    """

    Parameters:
        ZNRecord record
    """
    def __init__(self, record):
        super(record)


    def updateHistory(self, clusterName, instanceName):
        """
        Returns void
        Parameters:
            clusterName: StringinstanceName: String


        """
        # List<String>
        list = _record.getListField(clusterName)
        if list == None: 
            list = ArrayList<String>()
            _record.setListField(clusterName, list)

        if list.size() == HISTORY_SIZE: 
            list.remove(0)

        list.add(instanceName)


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        return True



