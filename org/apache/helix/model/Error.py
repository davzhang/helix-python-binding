# package org.apache.helix.model
#from org.apache.helix.model import *
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.ZNRecord import ZNRecord


class Error(HelixProperty):

    """

    Parameters:
        ZNRecord record
    """
    def __init__(self, record):
        super(Error,self).__init__(record)


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        return True



