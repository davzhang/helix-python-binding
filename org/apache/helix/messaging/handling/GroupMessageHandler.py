# package org.apache.helix.messaging.handling
#from org.apache.helix.messaging.handling import *
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from java.util.concurrent import ConcurrentHashMap
#from java.util.concurrent import ConcurrentLinkedQueue
#from java.util.concurrent.atomic import AtomicInteger
from org.apache.helix.PropertyKey import PropertyKey
from org.apache.helix.model.CurrentState import CurrentState
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import Attributes

class CurrentStateUpdate:

    """

    Parameters:
        PropertyKey key
        CurrentState curStateDelta
    """
    def __init__(self, key, curStateDelta):
        self._key = key
        self._curStateDelta = curStateDelta


    def merge(self, curState):
        """
        Returns void
        Parameters:
            curState: CurrentState


        """
        self._curStateDelta.getRecord().merge(curState.getRecord())


class GroupMessageInfo:

    """

    Parameters:
        Message message
    """
    def __init__(self, message):
        self._message = message
        # List<String>
        partitionNames = message.getPartitionNames()
        self._countDown = partitionNames.size()
        #            self._countDown = AtomicInteger(partitionNames.size())
        self._curStateUpdateList = []
#            self._curStateUpdateList = ConcurrentLinkedQueue<CurrentStateUpdate>()



    def merge(self):
        """
        Returns Map<PropertyKey, CurrentState>


        """
        # Map<String, CurrentStateUpdate>
        curStateUpdateMap = {}
        for update in self._curStateUpdateList: # String
            path = update._key.getPath()
            if not curStateUpdateMap.containsKey(path):
                curStateUpdateMap.put(path, update)
            else:
                curStateUpdateMap.get(path).merge(update._curStateDelta)


        # Map<PropertyKey, CurrentState>
        ret = {}
        for update in curStateUpdateMap.values():
            ret[update._key] = update._curStateDelta

        return ret

class GroupMessageHandler:


    """

    """
    def __init__(self):
#        self._groupMsgMap = ConcurrentHashMap<String, GroupMessageInfo>()
        self._groupMsgMap = {}


    def put(self, message):
        """
        Returns void
        Parameters:
            message: Message


        """
        self._groupMsgMap.putIfAbsent(message.getId(), GroupMessageInfo(message))


    def onCompleteSubMessage(self, subMessage):
        """
        Returns GroupMessageInfo
        Parameters:
            subMessage: Message


        """
        # String
        parentMid = subMessage.getAttribute(Attributes.PARENT_MSG_ID)
        # GroupMessageInfo
        info = self._groupMsgMap.get(parentMid)
        if info != None: 
            # int
            val = info._countDown.decrementAndGet()
            if val <= 0: 
                return self._groupMsgMap.remove(parentMid)


        return None


    def addCurStateUpdate(self, subMessage, key, delta):
        """
        Returns void
        Parameters:
            subMessage: Messagekey: PropertyKeydelta: CurrentState


        """
        # String
        parentMid = subMessage.getAttribute(Attributes.PARENT_MSG_ID)
        # GroupMessageInfo
        info = self._groupMsgMap.get(parentMid)
        if info != None: 
            info._curStateUpdateList.add(CurrentStateUpdate(key, delta))




