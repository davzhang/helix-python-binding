# package org.apache.helix
#from org.apache.helix import *

#class HelixException(RuntimeException):
class HelixException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
#
#    """
#
#    Parameters:
#        String message
#    """
#    def __init__(self, message):
#        super(message)
#
#
#    """
#
#    Parameters:
#        Throwable cause
#    """
#    def __init__(self, cause):
#        super(cause)



