# package org.apache.helix
#from org.apache.helix import *
#from java.util import ArrayList
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from java.util import TreeMap
#from org.codehaus.jackson.annotate import JsonProperty

@Deprecated

class ClusterView:



    def setClusterPropertyLists(self, clusterPropertyLists):
        """
        Returns void
        Parameters:
            clusterPropertyLists: Map<PropertyType, List<ZNRecord>>


        """
        self.clusterPropertyLists = clusterPropertyLists


    def getPropertyLists(self):
        """
        Returns Map<PropertyType, List<ZNRecord>>


        """
        return clusterPropertyLists


    def setClusterPropertyList(self, type, propertyList):
        """
        Returns void
        Parameters:
            type: PropertyTypepropertyList: List<ZNRecord>


        """
        clusterPropertyLists.put(type, propertyList)


    def getPropertyList(self, type):
        """
        Returns List<ZNRecord>
        Parameters:
            type: PropertyType


        """
        return clusterPropertyLists.get(type)


    def setMemberInstanceMap(self, memberInstanceMap):
        """
        Returns void
        Parameters:
            memberInstanceMap: Map<String, MemberInstance>


        """
        self._memberInstanceMap = memberInstanceMap


    def getMemberInstanceMap(self):
        """
        Returns Map<String, MemberInstance>
        @JsonProperty


        """
        return _memberInstanceMap


    def set_memberInstanceMap(self, _memberInstanceMap):
        """
        Returns void
        Parameters:
            _memberInstanceMap: Map<String, MemberInstance>


        """
        self._memberInstanceMap = _memberInstanceMap






    def setInstances(self, instances):
        """
        Returns void
        Parameters:
            instances: List<MemberInstance>


        """
        self._instances = instances


    def getInstances(self):
        """
        Returns List<MemberInstance>


        """
        return _instances


    Java modifiers:
         static
    class MemberInstance:



        def setClusterProperties(self, instanceProperties):
            """
            Returns void
            Parameters:
                instanceProperties: Map<PropertyType, List<ZNRecord>>


            """
            self._instanceProperties = instanceProperties


        def getInstanceProperties(self):
            """
            Returns Map<PropertyType, List<ZNRecord>>


            """
            return _instanceProperties


        def setInstanceProperty(self, type, values):
            """
            Returns void
            Parameters:
                type: PropertyTypevalues: List<ZNRecord>


            """
            _instanceProperties.put(type, values)


        def getInstanceProperty(self, type):
            """
            Returns List<ZNRecord>
            Parameters:
                type: PropertyType


            """
            return _instanceProperties.get(type)




        """

        """
        def __init__(self):


        """

        Parameters:
            String instanceName
        """
        def __init__(self, instanceName):
            self._instanceName = instanceName


        def getInstanceName(self):
            """
            Returns String


            """
            return _instanceName


        def setInstanceName(self, instanceName):
            """
            Returns void
            Parameters:
                instanceName: String


            """
            self._instanceName = instanceName



    def getMemberInstance(self, instanceName, createNewIfAbsent):
        """
        Returns MemberInstance
        Parameters:
            instanceName: StringcreateNewIfAbsent: boolean


        """
        if not _memberInstanceMap.containsKey(instanceName): 
            _memberInstanceMap.put(instanceName, MemberInstance(instanceName))

        return _memberInstanceMap.get(instanceName)




    """

    """
    def __init__(self):
        clusterPropertyLists = TreeMap<PropertyType, List<ZNRecord>>()
        setClusterPropertyList(PropertyType.IDEALSTATES, ArrayList<ZNRecord>())
        setClusterPropertyList(PropertyType.CONFIGS, ArrayList<ZNRecord>())
        setClusterPropertyList(PropertyType.LIVEINSTANCES, ArrayList<ZNRecord>())
        setClusterPropertyList(PropertyType.INSTANCES, ArrayList<ZNRecord>())
        self._memberInstanceMap = HashMap<String, ClusterView.MemberInstance>()


    def getExternalView(self):
        """
        Returns List<ZNRecord>


        """
        return _externalView


    def setExternalView(self, externalView):
        """
        Returns void
        Parameters:
            externalView: List<ZNRecord>


        """
        self._externalView = externalView



