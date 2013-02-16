# package org.apache.helix.store
#from org.apache.helix.store import *
#from java.util import Comparator
#from org.apache.log4j import Logger


# Parameterized type: <T>
class PropertyJsonComparator(Comparator<T>):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(PropertyJsonComparator.class)



    """

    Parameters:
        Class<T> clazz
    """
    def __init__(self, clazz):
        self._serializer = PropertyJsonSerializer<T>(clazz)


    def compare(self, arg0, arg1):
        """
        Returns int
        Parameters:
            arg0: Targ1: T
        @Override


        """
        if arg0 == None && arg1 == None: 
            return 0
        else:
            if arg0 == None && arg1 != None: 
                return -1
            else:
                if arg0 != None && arg1 == None: 
                    return 1
                else:
                    try:
                        # String
                        s0 = String(_serializer.serialize(arg0))
                        # String
                        s1 = String(_serializer.serialize(arg1))
                        return s0.compareTo(s1)
                    except PropertyStoreException, e:
                        LOG.warn(e.getMessage())
                        return -1





