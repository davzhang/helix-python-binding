# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import Map
#from java.util import Set
#from java.util import TreeMap
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.util.UserExceptions import IllegalArgumentException


class ExternalView(HelixProperty):

    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], str):
            self.__init_resource__(*args)
        elif len(args)==1 and isinstance(args[0], ZNRecord):
            self.__init_record__(*args)
        else:
            raise IllegalArgumentException("Input arguments not supported. args = %s" % args)
    """

    Parameters:
        String resource
    """
    def __init_resource__(self, resource):
        super(ExternalView,self).__init__(ZNRecord(resource))


    """

    Parameters:
        ZNRecord record
    """
    def __init_record__(self, record):
        super(ExternalView,self).__init__(record)


    def setState(self, partition, instance, state):
        """
        Returns void
        Parameters:
            partition: Stringinstance: Stringstate: String


        """
        if self._record.getMapField(partition) == None:
            self._record.setMapField(partition, {})
#            self._record.setMapField(partition, TreeMap<String, String>())

        self._record.getMapField(partition).put(instance, state)


    def setStateMap(self, partitionName, currentStateMap):
        """
        Returns void
        Parameters:
            partitionName: StringcurrentStateMap: Map<String, String>


        """
        self._record.setMapField(partitionName, currentStateMap)


    def getPartitionSet(self):
        """
        Returns Set<String>


        """
        return self._record.getMapFields().keySet()


    def getStateMap(self, partitionName):
        """
        Returns Map<String, String>
        Parameters:
            partitionName: String


        """
        return self._record.getMapField(partitionName)


    def getResourceName(self):
        """
        Returns String


        """
        return self._record.getId()


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        return True



