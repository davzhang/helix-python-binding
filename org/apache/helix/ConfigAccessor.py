# package org.apache.helix
#from org.apache.helix import *
#from java.util import ArrayList
#from java.util import Arrays
#from java.util import Collections
#from java.util import List
#from java.util import TreeMap
#from org.apache.log4j import Logger
import re

from org.apache.helix.ConfigScope import ConfigScopeProperty
from org.apache.helix.HelixException import HelixException
from org.apache.helix.manager.zk.ZKUtil import ZKUtil
from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.util.StringTemplate import StringTemplate
from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException

class ConfigAccessor:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)

    """
    Java modifiers:
         private final static
    Type:
        StringTemplate
    """
    template = StringTemplate()

    template.addEntry(ConfigScopeProperty.CLUSTER, 1, "/{clusterName}/CONFIGS/CLUSTER")
    template.addEntry(ConfigScopeProperty.CLUSTER, 2, "/{clusterName}/CONFIGS/CLUSTER/{clusterName}|SIMPLEKEYS")
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 1, "/{clusterName}/CONFIGS/PARTICIPANT")
    template.addEntry(ConfigScopeProperty.PARTICIPANT, 2, "/{clusterName}/CONFIGS/PARTICIPANT/{participantName}|SIMPLEKEYS")
    template.addEntry(ConfigScopeProperty.RESOURCE, 1, "/{clusterName}/CONFIGS/RESOURCE")
    template.addEntry(ConfigScopeProperty.RESOURCE, 2, "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|SIMPLEKEYS")
    template.addEntry(ConfigScopeProperty.PARTITION, 2, "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|MAPKEYS")
    template.addEntry(ConfigScopeProperty.PARTITION, 3, "/{clusterName}/CONFIGS/RESOURCE/{resourceName}|MAPMAPKEYS|{partitionName}")


    """

    Parameters:
        ZkClient zkClient
    """
    def __init__(self, zkClient):
        self.zkClient = zkClient


    def get(self, scope, key):
        """
        Returns String
        Parameters:
            scope: ConfigScopekey: String


        """
        if scope == None or scope.getScope() == None:
            self.LOG.error("Scope can't be null")
            return None

        # String
        value = None
        # String
        clusterName = scope.getClusterName()
        if not ZKUtil.isClusterSetup(clusterName, self.zkClient):
            raise HelixException("cluster " + clusterName + " is not setup yet")


        # String
        scopeStr = scope.getScopeStr()
        # String[]
#        splits = scopeStr.split("\|")
        splits = re.split("[\\|]", scopeStr)
        # ZNRecord
        record = self.zkClient.readData(splits[0], True)
        if record != None:
            if len(splits) == 1:
                value = record.getSimpleField(key)
            else:
                if splits.length == 2:
                    if record.getMapField(splits[1]) != None:
                        value = record.getMapField(splits[1]).get(key)

        return value
#
#
#    def set(self, scope, key, value):
#        """
#        Returns void
#        Parameters:
#            scope: ConfigScopekey: Stringvalue: String
#
#
#        """
#        if scope == None or scope.getScope() == None:
#            LOG.error("Scope can't be null")
#            return
#
#        # String
#        clusterName = scope.getClusterName()
#        if not ZKUtil.isClusterSetup(clusterName, self.zkClient):
#            raise HelixException("cluster " + clusterName + " is not setup yet")
#
#
#        # String
#        scopeStr = scope.getScopeStr()
#        # String[]
#        splits = scopeStr.split("\\|")
#        # String
#        id = splits[0].substring(splits[0].lastIndexOf('/') + 1)
#        # ZNRecord
#        update = ZNRecord(id)
#        if splits.length == 1:
#            update.setSimpleField(key, value)
#        else:
#            if splits.length == 2:
#                if update.getMapField(splits[1]) == None:
#                    update.setMapField(splits[1], TreeMap<String, String>())
#
#                update.getMapField(splits[1]).put(key, value)
#
#        ZKUtil.createOrUpdate(self.zkClient, splits[0], update, True, True)
#        return
#
#
#    def remove(self, scope, key):
#        """
#        Returns void
#        Parameters:
#            scope: ConfigScopekey: String
#
#
#        """
#        if scope == None or scope.getScope() == None:
#            LOG.error("Scope can't be null")
#            return
#
#        # String
#        clusterName = scope.getClusterName()
#        if not ZKUtil.isClusterSetup(clusterName, self.zkClient):
#            raise HelixException("cluster " + clusterName + " is not setup yet")
#
#
#        # String
#        scopeStr = scope.getScopeStr()
#        # String[]
#        splits = scopeStr.split("\\|")
#        # String
#        id = splits[0].substring(splits[0].lastIndexOf('/') + 1)
#        # ZNRecord
#        update = ZNRecord(id)
#        if splits.length == 1:
#            update.setSimpleField(key, "")
#        else:
#            if splits.length == 2:
#                if update.getMapField(splits[1]) == None:
#                    update.setMapField(splits[1], TreeMap<String, String>())
#
#                update.getMapField(splits[1]).put(key, "")
#
#        ZKUtil.subtract(self.zkClient, splits[0], update)
#        return
#
#
#    def getKeys(self, type, clusterName, keys):
#        """
#        Returns List<String>
#        Parameters:
#            type: ConfigScopePropertyclusterName: Stringkeys: String
#
#
#        """
#        if type == None or clusterName == None:
#            LOG.error("clusterName|scope can't be null")
#            return Collections.emptyList()
#
#        try:
#            if not ZKUtil.isClusterSetup(clusterName, self.zkClient):
#                LOG.error("cluster " + str(clusterName)+ " is not setup yet")
#                return Collections.emptyList()
#
#            # String[]
#            args = new String[1 + keys.length]
#            args[0] = clusterName
#            System.arraycopy(keys, 0, args, 1, keys.length)
#            # String
#            scopeStr = template.instantiate(type, args)
#            # String[]
#            splits = scopeStr.split("\\|")
#            # List<String>
#            retKeys = None
#            if splits.length == 1:
#                retKeys = self.zkClient.getChildren(splits[0])
#            else:
#                # ZNRecord
#                record = self.zkClient.readData(splits[0])
#                if splits[1].startsWith("SIMPLEKEYS"):
#                    retKeys = ArrayList<String>(record.getSimpleFields().keySet())
#                else:
#                    if splits[1].startsWith("MAPKEYS"):
#                        retKeys = ArrayList<String>(record.getMapFields().keySet())
#                    else:
#                        if splits[1].startsWith("MAPMAPKEYS"):
#                            retKeys = ArrayList<String>(record.getMapField(splits[2]).keySet())
#
#
#            if retKeys == None:
#                LOG.error("Invalid scope: " + str(type)+ " or keys: " + str(Arrays.toString(args)))
#                return Collections.emptyList()
#
#            Collections.sort(retKeys)
#            return retKeys
#        except Exception, e:
#            return Collections.emptyList()
#
#
#
#
