'''
  A sample Helix Spectator
'''
from org.apache.helix.ExternalViewChangeListener import ExternalViewChangeListener
import time

class ExampleEVListener(ExternalViewChangeListener):
    def __init__(self):
        self.externalViewList = {}
        self.externalViewChangeTimeStamp = 0

    def onExternalViewChange(self, externalViewList, changeContext):
        self.externalViewChangeTimeStamp = str(time.time())
        self.externalViewList = externalViewList

    def getResources(self):
        ''' get the resources
        '''
        if not self.externalViewList: return []
        return [ev.getResourceName() for ev in self.externalViewList]

    def getNumPartitions(self):
        if not self.externalViewList: return []
        return [len(ev._record.getMapFields()) for ev in self.externalViewList]

    def getTimeStamp(self):
        return self.externalViewChangeTimeStamp

    def getEV(self, resourceName):
        resources = self.getResources()
        if resourceName not in resources: return None
        #        return json.dumps(self.externalViewList[resourceName]._record.getMapFields())
        return self.externalViewList[resources.index(resourceName)]._record.getMapFields()
