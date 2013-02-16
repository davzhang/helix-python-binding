# package org.apache.helix.store
#from org.apache.helix.store import *

# Parameterized type: <T>
class PropertySerializer():

    def serialize(self, data):
        """
        Returns byte[]
        Parameters:
            data: T


        Throws: 
            PropertyStoreException
        """
        pass


    def deserialize(self, bytes):
        """
        Returns T
        Parameters:
            bytes: byte[]


        Throws: 
            PropertyStoreException
        """
        pass



