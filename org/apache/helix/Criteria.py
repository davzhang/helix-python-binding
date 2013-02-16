# package org.apache.helix
#from org.apache.helix import *
from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum

DataSource=enum('IDEALSTATES', 'EXTERNALVIEW')

class Criteria:

    def getDataSource(self):
        """
        Returns DataSource


        """
        return self._dataSource


    def setDataSource(self, source):
        """
        Returns void
        Parameters:
            source: DataSource


        """
        _dataSource = source


    def isSelfExcluded(self):
        """
        Returns boolean


        """
        return self.selfExcluded


    def setSelfExcluded(self, selfExcluded):
        """
        Returns void
        Parameters:
            selfExcluded: boolean


        """
        self.selfExcluded = selfExcluded


    def getRecipientInstanceType(self):
        """
        Returns InstanceType


        """
        return self.recipientInstanceType


    def setRecipientInstanceType(self, recipientInstanceType):
        """
        Returns void
        Parameters:
            recipientInstanceType: InstanceType


        """
        self.recipientInstanceType = recipientInstanceType


    def isSessionSpecific(self):
        """
        Returns boolean


        """
        return self.sessionSpecific


    def setSessionSpecific(self, sessionSpecific):
        """
        Returns void
        Parameters:
            sessionSpecific: boolean


        """
        self.sessionSpecific = sessionSpecific


    def getInstanceName(self):
        """
        Returns String


        """
        return self.instanceName


    def setInstanceName(self, instanceName):
        """
        Returns void
        Parameters:
            instanceName: String


        """
        self.instanceName = instanceName


    def getResource(self):
        """
        Returns String


        """
        return self.resourceName


    def setResource(self, resourceName):
        """
        Returns void
        Parameters:
            resourceName: String


        """
        self.resourceName = resourceName


    def getPartition(self):
        """
        Returns String


        """
        return self.partitionName


    def setPartition(self, partitionName):
        """
        Returns void
        Parameters:
            partitionName: String


        """
        self.partitionName = partitionName


    def getPartitionState(self):
        """
        Returns String


        """
        return self.partitionState


    def setPartitionState(self, partitionState):
        """
        Returns void
        Parameters:
            partitionState: String


        """
        self.partitionState = partitionState


    def toString(self):
        """
        Returns String


        """
        # StringBuilder
#        sb = StringBuilder()
        sb = []
        sb.append("instanceName").append("=").append(self.instanceName)
        sb.append("resourceName").append("=").append(self.resourceName)
        sb.append("partitionName").append("=").append(self.partitionName)
        sb.append("partitionState").append("=").append(self.partitionState)
#        return sb.toString()
        return "".join(sb)

    def __str__(self):
        return self.toString()




