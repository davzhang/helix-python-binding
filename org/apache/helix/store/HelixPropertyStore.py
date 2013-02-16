# package org.apache.helix.store
#from org.apache.helix.store import *
from org.apache.helix.BaseDataAccessor import BaseDataAccessor


# Parameterized type: <T>
class HelixPropertyStore(BaseDataAccessor<T>):

    def start(self):
        """
        Returns void


        """
        pass


    def stop(self):
        """
        Returns void


        """
        pass


    def subscribe(self, parentPath, listener):
        """
        Returns void
        Parameters:
            parentPath: Stringlistener: HelixPropertyListener


        """
        pass


    def unsubscribe(self, parentPath, listener):
        """
        Returns void
        Parameters:
            parentPath: Stringlistener: HelixPropertyListener


        """
        pass



