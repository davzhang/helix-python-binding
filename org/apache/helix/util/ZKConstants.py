'''
  Zookeeper Constant and Enum
'''


class CreateMode(object):

    Properties = {}
# The znode will not be automatically deleted upon client's disconnect.
    PERSISTENT = 0
    Properties[PERSISTENT] = (False, False)
# The znode will not be automatically deleted upon client's disconnect,
# and its name will be appended with a monotonically increasing number.
    PERSISTENT_SEQUENTIAL=2
    Properties[PERSISTENT_SEQUENTIAL] = (False, True)
# The znode will be deleted upon the client's disconnect.
    EPHEMERAL  = 1
    Properties[EPHEMERAL] = (True, False)
# The znode will be deleted upon the client's disconnect, and its name
# will be appended with a monotonically increasing number.
    EPHEMERAL_SEQUENTIAL =  3
    Properties[EPHEMERAL_SEQUENTIAL] = (True, True)

    @staticmethod
    def isEphemeral(mode):
        return CreateMode.Properties[mode][0]

    @staticmethod
    def isSequential(mode):
        return CreateMode.Properties[mode][1]

class HelixZNodeStat(object):
    '''
    A wrapper class around kazoo.ZNodeStat
    '''
    def __init__(self, czxid = None, mzxid = None, ctime = None, mtime = None, version = None, cversion = None, aversion = None, ephemeralOwner = None, dataLength = None, numChildren = None, pzxid = None):
      self.czxid = czxid
      self.mzxid = mzxid
      self.ctime = ctime
      self.mtime = mtime
      self.version = version
      self.cversion = cversion
      self.aversion = aversion
      self.ephemeralOwner = ephemeralOwner
      self.dataLength = dataLength
      self.numChildren = numChildren
      self.pzxid = pzxid

    def getCzxid(self): return self.czxid
    def getMzxid(self): return self.mzxid
    def getCtime(self): return self.ctime
    def getMtime(self): return self.mtime
    def getVersion(self): return self.version
    def getCversion(self): return self.cversion
    def getAversion(self): return self.aversion
    def getEphemeralOwner(self): return self.ephemeralOwner
    def getEataLength(self): return self.dataLength
    def getNumChildren(self): return self.numChildren
    def getPzxid(self): return self.pzxid
