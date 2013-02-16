# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import ArrayList
#from java.util import Collections
#from java.util import Comparator
#from java.util import Date
#from java.util import List
#from java.util import Map
#from java.util import UUID
from org.apache.helix.HelixException import HelixException
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.InstanceType import InstanceType
#from org.apache.helix.PropertyKey import PropertyKey
#from org.apache.helix.PropertyKey import Builder
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum
import time, uuid

MessageType = enum('STATE_TRANSITION', 'SCHEDULER_MSG', 'USER_DEFINE_MSG', 'CONTROLLER_MSG', 'TASK_REPLY', 'NO_OP', 'PARTICIPANT_ERROR_REPORT')

Attributes = enum('MSG_ID, SRC_SESSION_ID', 'TGT_SESSION_ID', 'SRC_NAME', 'TGT_NAME', 'SRC_INSTANCE_TYPE', 'MSG_STATE',
    'PARTITION_NAME', 'RESOURCE_NAME', 'FROM_STATE', 'TO_STATE', 'STATE_MODEL_DEF',
    'CREATE_TIMESTAMP', 'READ_TIMESTAMP', 'EXECUTE_START_TIMESTAMP',
    'MSG_TYPE', 'MSG_SUBTYPE', 'CORRELATION_ID', 'MESSAGE_RESULT',
    'EXE_SESSION_ID', 'TIMEOUT', 'RETRY_COUNT', 'STATE_MODEL_FACTORY_NAME', 'BUCKET_SIZE', 'PARENT_MSG_ID')

MessageState =enum('NEW', 'READ', 'UNPROCESSABLE')


class Message(HelixProperty):

#    Attributes = enum('MSG_ID, SRC_SESSION_ID', 'TGT_SESSION_ID', 'SRC_NAME', 'TGT_NAME', 'SRC_INSTANCE_TYPE', 'MSG_STATE',
#    'PARTITION_NAME', 'RESOURCE_NAME', 'FROM_STATE', 'TO_STATE', 'STATE_MODEL_DEF',
#    'CREATE_TIMESTAMP', 'READ_TIMESTAMP', 'EXECUTE_START_TIMESTAMP',
#    'MSG_TYPE', 'MSG_SUBTYPE', 'CORRELATION_ID', 'MESSAGE_RESULT',
#    'EXE_SESSION_ID', 'TIMEOUT', 'RETRY_COUNT', 'STATE_MODEL_FACTORY_NAME', 'BUCKET_SIZE', 'PARENT_MSG_ID')


    """
    Java modifiers:
         final static
    Type:
        Comparator<Message>
    """
    @staticmethod
    def compare(m1, m2):
        """
        Returns int
        Parameters:
            m1: Messagem2: Message
        @Override


        """
        return int(long(m1.getCreateTimeStamp()) - long(m2.getCreateTimeStamp()))

    CREATE_TIME_COMPARATOR = compare

    def __init__(self, *args):
        if len(args)==2 and isinstance(args[1], str):
            self.__init__type_msgId(*args)
        elif len(args)==1 and isinstance(args[0], ZNRecord):
            self.__init__record(*args)
        elif len(args)==2 and isinstance(args[0], ZNRecord):
            self.__init__record_id(*args)
        else:
            raise IllegalArgumentException("Input arguments not supported. args = %s" % args)


    """

    Parameters:
        MessageType type
        String msgId
    """
    def __init__type_msgId(self, type, msgId):
        super(Message,self).__init__(msgId)
        self._record.setSimpleField("MSG_TYPE", MessageType.toString(type))
        self.setMsgId(msgId)
        self.setMsgState(MessageState.NEW)
        self._record.setSimpleField('CREATE_TIMESTAMP', time.time())


    """

    Parameters:
        ZNRecord record
    """
    def __init__record(self, record):
        super(Message,self).__init__(record)
        if self.getMsgState() == None:
            self.setMsgState(MessageState.NEW)

        if self.getCreateTimeStamp() == 0: 
            self._record.setSimpleField("CREATE_TIMESTAMP", "" + time.time())



    def setCreateTimeStamp(self, timestamp):
        """
        Returns void
        Parameters:
            timestamp: long


        """
        self._record.setSimpleField("CREATE_TIMESTAMP", "" + timestamp)


    """

    Parameters:
        ZNRecord record
        String id
    """
    def __init__record_id(self, record, id):
        super(Message,self).__init__(ZNRecord(record,id))
#        super(ZNRecord(record, id))
        self.setMsgId(id)


    def setMsgSubType(self, subType):
        """
        Returns void
        Parameters:
            subType: String


        """
        self._record.setSimpleField("MSG_SUBTYPE", subType)


    def getMsgSubType(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("MSG_SUBTYPE")


    def setMsgType(self, type):
        """
        Returns void
        Parameters:
            type: MessageType


        """
        self._record.setSimpleField("MSG_TYPE", type.toString())


    def getMsgType(self):
        """
        Returns String


        """
        return self._record.getSimpleField("MSG_TYPE")


    def getTgtSessionId(self):
        """
        Returns String


        """
        return self._record.getSimpleField("TGT_SESSION_ID")


    def setTgtSessionId(self, tgtSessionId):
        """
        Returns void
        Parameters:
            tgtSessionId: String


        """
        self._record.setSimpleField("TGT_SESSION_ID", tgtSessionId)


    def getSrcSessionId(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("SRC_SESSION_ID")


    def setSrcSessionId(self, srcSessionId):
        """
        Returns void
        Parameters:
            srcSessionId: String


        """
        self._record.setSimpleField("SRC_SESSION_ID", srcSessionId)


    def getExecutionSessionId(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("EXE_SESSION_ID")


    def setExecuteSessionId(self, exeSessionId):
        """
        Returns void
        Parameters:
            exeSessionId: String


        """
        self._record.setSimpleField("EXE_SESSION_ID", exeSessionId)


    def getMsgSrc(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("SRC_NAME")


    def setSrcInstanceType(self, type):
        """
        Returns void
        Parameters:
            type: InstanceType


        """
        self._record.setSimpleField("SRC_INSTANCE_TYPE", type.toString())


    def getSrcInstanceType(self):
        """
        Returns InstanceType


        """
        if  self._record.getSimpleFields().containsKey("SRC_INSTANCE_TYPE"): 
            return InstanceType.valueOf(self._record.getSimpleField("SRC_INSTANCE_TYPE"))

        return InstanceType.PARTICIPANT




    def setSrcName(self, msgSrc):
        """
        Returns void
        Parameters:
            msgSrc: String


        """
        self._record.setSimpleField("SRC_NAME", msgSrc)


    def getTgtName(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("TGT_NAME")


    def setMsgState(self, msgState):
        """
        Returns void
        Parameters:
            msgState: MessageState


        """
        self._record.setSimpleField("MSG_STATE", MessageState.toString(msgState).lower())


    def getMsgState(self):
        """
        Returns MessageState


        """
        return getattr(MessageState, self._record.getSimpleField("MSG_STATE").upper())

    def setPartitionName(self, partitionName):
        """
        Returns void
        Parameters:
            partitionName: String


        """
        self._record.setSimpleField("PARTITION_NAME", partitionName)


    def getMsgId(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("MSG_ID")


    def setMsgId(self, msgId):
        """
        Returns void
        Parameters:
            msgId: String


        """
        self._record.setSimpleField("MSG_ID", msgId)


    def setFromState(self, state):
        """
        Returns void
        Parameters:
            state: String


        """
        self._record.setSimpleField("FROM_STATE", state)


    def getFromState(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("FROM_STATE")


    def setToState(self, state):
        """
        Returns void
        Parameters:
            state: String


        """
        self._record.setSimpleField("TO_STATE", state)


    def getToState(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("TO_STATE")


    def setTgtName(self, msgTgt):
        """
        Returns void
        Parameters:
            msgTgt: String


        """
        self._record.setSimpleField("TGT_NAME", msgTgt)


    def getDebug(self):
        """
        Returns Boolean


        """
        return False


    def getGeneration(self):
        """
        Returns Integer


        """
        return 1


    def setResourceName(self, resourceName):
        """
        Returns void
        Parameters:
            resourceName: String


        """
        self._record.setSimpleField("RESOURCE_NAME", resourceName)


    def getResourceName(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("RESOURCE_NAME")


    def getPartitionName(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("PARTITION_NAME")


    def getStateModelDef(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("STATE_MODEL_DEF")


    def setStateModelDef(self, stateModelDefName):
        """
        Returns void
        Parameters:
            stateModelDefName: String


        """
        self._record.setSimpleField("STATE_MODEL_DEF", stateModelDefName)


    def setReadTimeStamp(self, time):
        """
        Returns void
        Parameters:
            time: long


        """
        self._record.setSimpleField("READ_TIMESTAMP", "" + str(time))


    def setExecuteStartTimeStamp(self, time):
        """
        Returns void
        Parameters:
            time: long


        """
        self._record.setSimpleField("EXECUTE_START_TIMESTAMP", "" + str(time))


    def getReadTimeStamp(self):
        """
        Returns long


        """
        # String
        timestamp =  self._record.getSimpleField("READ_TIMESTAMP")
        if timestamp == None: 
            return 0
        else: return timestamp

    def getExecuteStartTimeStamp(self):
        """
        Returns long


        """
        # String
        timestamp = self._record.getSimpleField("EXECUTE_START_TIMESTAMP")
        if timestamp == None: 
            return 0
        else:
            return timestamp


    def getCreateTimeStamp(self):
        """
        Returns long


        """
        timestamp =  self._record.getSimpleField("CREATE_TIMESTAMP")
        if timestamp == None: 
            return 0
        else:
            return timestamp


    def setCorrelationId(self, correlationId):
        """
        Returns void
        Parameters:
            correlationId: String


        """
        self._record.setSimpleField("CORRELATION_ID", correlationId)


    def getCorrelationId(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("CORRELATION_ID")


    def getExecutionTimeout(self):
        """
        Returns int


        """
        if not "TIMEOUT" in self._record.getSimpleFields():
            return -1

        return self._record.getSimpleField("TIMEOUT")


    def setExecutionTimeout(self, timeout):
        """
        Returns void
        Parameters:
            timeout: int


        """
        self._record.setSimpleField("TIMEOUT", "" + str(timeout))


    def setRetryCount(self, retryCount):
        """
        Returns void
        Parameters:
            retryCount: int


        """
        self._record.setSimpleField("RETRY_COUNT", "" + str(retryCount))


    def getRetryCount(self):
        """
        Returns int


        """
        return self._record.getSimpleField("RETRY_COUNT")


    def getResultMap(self):
        """
        Returns Map<String, String>


        """
        return  self._record.getMapField("MESSAGE_RESULT")


    def setResultMap(self, resultMap):
        """
        Returns void
        Parameters:
            resultMap: Map<String, String>


        """
        self._record.setMapField("MESSAGE_RESULT", resultMap)


    def getStateModelFactoryName(self):
        """
        Returns String


        """
        return  self._record.getSimpleField("STATE_MODEL_FACTORY_NAME")


    def setStateModelFactoryName(self, factoryName):
        """
        Returns void
        Parameters:
            factoryName: String


        """
        self._record.setSimpleField("STATE_MODEL_FACTORY_NAME", factoryName)


    def getBucketSize(self):
        """
        Returns int
        @Override


        """
        # String
        bucketSizeStr =  self._record.getSimpleField("BUCKET_SIZE")
        # int
        bucketSize = 0
        if bucketSizeStr != None:
            try:
                bucketSize = int(bucketSizeStr)
            except ValueError, e: pass

        return bucketSize


    def setBucketSize(self, bucketSize):
        """
        Returns void
        Parameters:
            bucketSize: int
        @Override


        """
        if bucketSize > 0: 
             self._record.setSimpleField("BUCKET_SIZE", "" + str(bucketSize))



    def setAttribute(self, attr, val):
        """
        Returns void
        Parameters:
            attr: Attributesval: String


        """
        self._record.setSimpleField(attr.toString(), val)


    def getAttribute(self, attr):
        """
        Returns String
        Parameters:
            attr: Attributes


        """
        return  self._record.getSimpleField(attr.toString())


    def createReplyMessage(srcMessage, instanceName, taskResultMap):
        """
        Returns Message
        Parameters:
            srcMessage: MessageinstanceName: StringtaskResultMap: Map<String, String>
        Java modifiers:
             static

        """
        if srcMessage.getCorrelationId() == None: 
            raise HelixException("Message " + srcMessage.getMsgId() + " does not contain correlation id")


        # Message
        replyMessage = Message(MessageType.TASK_REPLY, str(uuid.uuid4()))
        replyMessage.setCorrelationId(srcMessage.getCorrelationId())
        replyMessage.setResultMap(taskResultMap)
        replyMessage.setTgtSessionId("*")
        replyMessage.setMsgState(MessageState.NEW)
        replyMessage.setSrcName(instanceName)
        if srcMessage.getSrcInstanceType() == InstanceType.CONTROLLER: 
            replyMessage.setTgtName("Controller")
        else:
            replyMessage.setTgtName(srcMessage.getMsgSrc())

        return replyMessage


    def addPartitionName(self, partitionName):
        """
        Returns void
        Parameters:
            partitionName: String


        """
        if  self._record.getListField("PARTITION_NAME") == None: 
             self._record.setListField("PARTITION_NAME", [])

        # List<String>
        partitionNames =  self._record.getListField("PARTITION_NAME")
        if not partitionNames.contains(partitionName): 
            partitionNames.add(partitionName)



    def getPartitionNames(self):
        """
        Returns List<String>


        """
        # List<String>
        partitionNames =  self._record.getListField("PARTITION_NAME")
        if partitionNames == None: 
            return []

        return partitionNames


    def isControlerMsg(self):
        """
        Returns boolean


        """
        return self.getTgtName().lower() == "controller"


    def getKey(self, keyBuilder, instanceName):
        """
        Returns PropertyKey
        Parameters:
            keyBuilder: BuilderinstanceName: String


        """
        if self.isControlerMsg():
            return keyBuilder.controllerMessage(self.getId())
        else:
            return keyBuilder.message(instanceName, self.getId())



    def isNullOrEmpty(self, data):
        """
        Returns boolean
        Parameters:
            data: String
        Java modifiers:
             private

        """
        return data == None or len(data) == 0 or len(data.strip()) == 0


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        if (self.getMsgType() == MessageType.toString(MessageType.STATE_TRANSITION)):
            # boolean
            isNotValid = self.isNullOrEmpty(self.getTgtName()) or self.isNullOrEmpty(self.getPartitionName()) or self.isNullOrEmpty(self.getResourceName()) or self.isNullOrEmpty(self.getStateModelDef()) or self.isNullOrEmpty(self.getToState()) or self.isNullOrEmpty(self.getStateModelFactoryName()) or self.isNullOrEmpty(self.getFromState())
            return not isNotValid

        return True



