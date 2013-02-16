# package org.apache.helix
#from org.apache.helix import *
#from java.util import HashMap
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.ConfigScope import ConfigScopeProperty, ConfigScope
from org.apache.helix.util.StringTemplate import StringTemplate
from org.apache.helix.util.UserExceptions import IllegalArgumentException

from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum

class ConfigScopeBuilder:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)

    """
    Java modifiers:
         private static
    Type:
        StringTemplate
    """
    template = StringTemplate()

    template.addEntry(ConfigScopeProperty.CLUSTER, 1, "CLUSTER={clusterName}")
    template.addEntry(ConfigScopeProperty.RESOURCE, 2, "CLUSTER={clusterName},RESOURCE={resourceName}")
    template.addEntry(ConfigScopeProperty.PARTITION, 3, "CLUSTER={clusterName},RESOURCE={resourceName},PARTITION={partitionName}")
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 2, "CLUSTER={clusterName},PARTICIPANT={participantName}")




    def getScopeMap(self):
        """
        Returns Map<ConfigScopeProperty, String>


        """
        return self._scopeMap


    """

    """
    def __init__(self):
        self._scopeMap = {}
#        self._scopeMap = HashMap<ConfigScopeProperty, String>()


    def forCluster(self, clusterName):
        """
        Returns ConfigScopeBuilder
        Parameters:
            clusterName: String


        """
        self._scopeMap.__setitem__(ConfigScopeProperty.CLUSTER, clusterName)
        return self


    def forParticipant(self, participantName):
        """
        Returns ConfigScopeBuilder
        Parameters:
            participantName: String


        """
        self._scopeMap.__setitem__(ConfigScopeProperty.PARTICIPANT, participantName)
        return self


    def forResource(self, resourceName):
        """
        Returns ConfigScopeBuilder
        Parameters:
            resourceName: String


        """
        self._scopeMap.__setitem__(ConfigScopeProperty.RESOURCE, resourceName)
        return self


    def forPartition(self, partitionName):
        """
        Returns ConfigScopeBuilder
        Parameters:
            partitionName: String


        """
        self._scopeMap.__setitem__(ConfigScopeProperty.PARTITION, partitionName)
        return self


    def build(self, *args):
        """
        Returns ConfigScope


        """
        if len(args)==0:
            return ConfigScope(self)
        elif len(args)==3:
            return self.build_3(*args)
        elif len(args)==1:
            return self.build_1(*args)
        else:
            raise IllegalArgumentException("Wrong args: %s" % args)


    def build_3(self, scope, clusterName, scopeKeys):
        """
        Returns ConfigScope
        Parameters:
            scope: ConfigScopePropertyclusterName: StringscopeKeys: String


        """
        if scopeKeys == None: 
            scopeKeys = {}
#            scopeKeys = new String[] {}

        # String[]
#        args = new String[1 + scopeKeys.length]
#        args[0] = clusterName
#        System.arraycopy(scopeKeys, 0, args, 1, scopeKeys.length)
        args=[clusterName]
        args.extend(scopeKeys[1:])
        # String
        scopePairs = ConfigScopeBuilder.template.instantiate(scope, args)
        return self.build(scopePairs)


    def build_1(self, scopePairs):
        """
        Returns ConfigScope
        Parameters:
            scopePairs: String


        """
        # String[]
#        scopes = scopePairs.split("[\\s,]+")
        # TODO: re split
        scopes = scopePairs.split("[\s,]+")
        for scope in scopes:
            try:
                # int
                idx = scope.indexOf('=')
                if idx == -1: 
                    self.LOG.error("Invalid scope string: " + str(scope))
                    continue

                # String
                scopeStr = scope.substring(0, idx)
                # String
                value = scope.substring(idx + 1)
                # ConfigScopeProperty
                scopeProperty = ConfigScopeProperty.valueOf(scopeStr)
                self._scopeMap.put(scopeProperty, value)
            except Exception, e:
                self.LOG.error("Invalid scope string: " + str(scope))
                continue


        return self.build()


    def toString(self):
        """
        Returns String
        @Override


        """
        return self._scopeMap.toString()



