# package org.apache.helix
#from org.apache.helix import *
#from java.util import ArrayList
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.util.StringTemplate import StringTemplate
import logging
from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum

ConfigScopeProperty=enum('CLUSTER', 'PARTICIPANT', 'RESOURCE', 'PARTITION', 'CONSTRAINT')


class ConfigScope:



    """
    Java modifiers:
         private static
    Type:
        Logger
    """
#    LOG = Logger.getLogger(ConfigScope.class)
    LOG = get_logger(__name__)

    """
    Java modifiers:
         private final static
    Type:
        List<ConfigScopeProperty>
    """
#    scopePriority = ArrayList<ConfigScopeProperty>()
    scopePriority = []

    """
    Java modifiers:
         private final static
    Type:
        Map<ConfigScopeProperty, Map<ConfigScopeProperty, ConfigScopeProperty>>
    """
    scopeTransition = {}
#    scopeTransition = HashMap<ConfigScopeProperty, Map<ConfigScopeProperty, ConfigScopeProperty>>()

    """
    Java modifiers:
         private final static
    Type:
        StringTemplate
    """
    template = StringTemplate()

    scopePriority.append(ConfigScopeProperty.CLUSTER)
    scopePriority.append(ConfigScopeProperty.PARTICIPANT)
    scopePriority.append(ConfigScopeProperty.RESOURCE)
    scopePriority.append(ConfigScopeProperty.PARTITION)
#    scopeTransition.__setitem__(ConfigScopeProperty.CLUSTER, HashMap<ConfigScopeProperty, ConfigScopeProperty>())
    scopeTransition.__setitem__(ConfigScopeProperty.CLUSTER, {})
    scopeTransition.get(ConfigScopeProperty.CLUSTER).__setitem__(ConfigScopeProperty.PARTICIPANT, ConfigScopeProperty.PARTICIPANT)
    scopeTransition.get(ConfigScopeProperty.CLUSTER).__setitem__(ConfigScopeProperty.RESOURCE, ConfigScopeProperty.RESOURCE)
    scopeTransition.__setitem__(ConfigScopeProperty.RESOURCE, {})
#    scopeTransition.put(ConfigScopeProperty.RESOURCE, HashMap<ConfigScopeProperty, ConfigScopeProperty>())
    scopeTransition.get(ConfigScopeProperty.RESOURCE).__setitem__(ConfigScopeProperty.PARTITION, ConfigScopeProperty.PARTITION)
    template.addEntry(ConfigScopeProperty.CLUSTER, 2, "/{clusterName}/CONFIGS/CLUSTER/{clusterName}")
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 2, "/{clusterName}/CONFIGS/PARTICIPANT/{participantName}")
    template.addEntry(ConfigScopeProperty.RESOURCE, 2, "/{clusterName}/CONFIGS/RESOURCE/{resourceName}")
    template.addEntry(ConfigScopeProperty.PARTITION, 3, "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|{partitionName}")


    """

    Parameters:
        ConfigScopeBuilder configScopeBuilder
    """
    def __init__(self, configScopeBuilder):
        # Map<ConfigScopeProperty, String>
        scopeMap = configScopeBuilder.getScopeMap()
        # List<String>
        keys = []
#        keys = ArrayList<String>()
        # ConfigScopeProperty
        curScope = None
        for scope in self.scopePriority:
            if scopeMap.__contains__(scope):
                if curScope == None and scope == ConfigScopeProperty.CLUSTER:
                    keys.append(scopeMap.get(scope))
                    curScope = ConfigScopeProperty.CLUSTER
                else:
                    if curScope == None: 
                        raise IllegalArgumentException("Missing CLUSTER scope. Can't build scope using " + configScopeBuilder)

                    else:
                        if not self.scopeTransition.__contains__(curScope) or not self.scopeTransition.get(curScope).__contains__(scope):
                            raise IllegalArgumentException("Can't build scope using " + configScopeBuilder)


                        keys.append(scopeMap.get(scope))
                        curScope = self.scopeTransition.get(curScope).get(scope)



        if curScope == ConfigScopeProperty.CLUSTER: 
            keys.append(scopeMap.get(ConfigScopeProperty.CLUSTER))

        # String
        scopeStr = self.template.instantiate(curScope, keys)
#        scopeStr = self.template.instantiate(curScope, keys.toArray(new String[0]))
        self._clusterName = keys[0]
        self._scopeStr = scopeStr
        self._scope = curScope


    def getScope(self):
        """
        Returns ConfigScopeProperty


        """
        return self._scope


    def getClusterName(self):
        """
        Returns String


        """
        return self._clusterName


    def getScopeStr(self):
        """
        Returns String


        """
        return self._scopeStr


#    def toString(self):
#        """
#        Returns String
#        @Override
#
#
#        """
#        return super(null, self).toString() + ": " + _scopeStr

    def __repr__(self):
        """
        Returns String
        @Override


        """
        return  ": " + self._scopeStr


