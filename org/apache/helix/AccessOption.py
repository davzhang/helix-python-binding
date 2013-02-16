# package org.apache.helix
#from org.apache.helix import *
#from org.apache.zookeeper import CreateMode
from org.apache.helix.util.ZKConstants import CreateMode


class AccessOption:

    """
    Java modifiers:
         static
    Type:
        int
    """
    PERSISTENT = 1

    """
    Java modifiers:
         static
    Type:
        int
    """
    EPHEMERAL = 2

    """
    Java modifiers:
         static
    Type:
        int
    """
    PERSISTENT_SEQUENTIAL = 4

    """
    Java modifiers:
         static
    Type:
        int
    """
    EPHEMERAL_SEQUENTIAL = 8

    """
    Java modifiers:
         static
    Type:
        int
    """
    THROW_EXCEPTION_IFNOTEXIST = 16

    @staticmethod
    def getMode(options):
        """
        Returns CreateMode
        Parameters:
            options: int
        Java modifiers:
             static

        """
        if (options & AccessOption.PERSISTENT) > 0:
            return CreateMode.PERSISTENT
        else:
            if (options & AccessOption.EPHEMERAL) > 0:
                return CreateMode.EPHEMERAL
            else:
                if (options & AccessOption.PERSISTENT_SEQUENTIAL) > 0:
                    return CreateMode.PERSISTENT_SEQUENTIAL
                else:
                    if (options & AccessOption.EPHEMERAL_SEQUENTIAL) > 0:
                        return CreateMode.EPHEMERAL_SEQUENTIAL

        return None


    @staticmethod
    def isThrowExceptionIfNotExist(options):
        """
        Returns boolean
        Parameters:
            options: int
        Java modifiers:
             static

        """
        return (options & AccessOption.THROW_EXCEPTION_IFNOTEXIST) > 0



