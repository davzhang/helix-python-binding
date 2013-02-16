# package org.apache.helix.util
#from org.apache.helix.util import *
#from java.io import PrintWriter
#from java.io import StringWriter
#from java.text import DateFormat
#from java.text import SimpleDateFormat
#from java.util import ArrayList
#from java.util import Collections
#from java.util import Date
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from java.util import TreeMap
#from java.util import UUID
#from java.util.concurrent import ConcurrentHashMap
#from org.apache.log4j import Logger
import uuid
from org.apache.helix.HelixDataAccessor import HelixDataAccessor
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.PropertyKey import PropertyKey
#from org.apache.helix.PropertyKey import Builder
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.model.Error import Error
from org.apache.helix.model.Message import Message
from org.apache.helix.model.Message import MessageType
from org.apache.helix.model.StatusUpdate import StatusUpdate

from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum
import time

Level=enum('HELIX_ERROR', 'HELIX_WARNING', 'HELIX_INFO')

TaskStatus=enum('UNKNOWN', 'NEW', 'SCHEDULED', 'INVOKING', 'COMPLETED', 'FAILED')


#class Transition(Comparable<Transition>):
class Transition():

    """

    Parameters:
        String msgID
        long timeStamp
        String from
        String to
    """
    def __init__(self, msgID, timeStamp, fromState, to):
        self._msgID = msgID
        self._timeStamp = timeStamp
        self._from = fromState
        self._to = to


    def compareTo(self, t):
        """
        Returns int
        Parameters:
            t: Transition
        @Override


        """
        if self._timeStamp < t._timeStamp:
            return -1
        elif self._timeStamp > t._timeStamp:
            return 1
        else:
            return 0


    def equals(self, t):
        """
        Returns boolean
        Parameters:
            t: Transition


        """
        return (self._timeStamp == t._timeStamp and (self._from == t._from) and (self._to == t._to))


    def getFromState(self):
        """
        Returns String


        """
        return self._from


    def getToState(self):
        """
        Returns String


        """
        return self._to


    def getMsgID(self):
        """
        Returns String


        """
        return self._msgID


    def toString(self):
        """
        Returns String
        @Override


        """
        return self._msgID + ":" + self._timeStamp + ":" + self._from + "->" + self._to

    def __str__(self):
        return self.toString()



class StatusUpdateContents:

    """
    Java modifiers:
         private
    Parameters:
        List<Transition> transitions
        Map<String, TaskStatus> taskMessages
    """
    def __init__(self, transitions, taskMessages):
        self._transitions = transitions
        self._taskMessages = taskMessages


    def getStatusUpdateContents(self, accessor, instance, resourceGroup, partition):
        """
        Returns StatusUpdateContents
        Parameters:
            accessor: HelixDataAccessorinstance: StringresourceGroup: Stringpartition: String
        Java modifiers:
             static

        """
        return self.getStatusUpdateContents(accessor, instance, resourceGroup, None, partition)


    def getStatusUpdateContents(self, accessor, instance, resourceGroup, sessionID, partition):
        """
        Returns StatusUpdateContents
        Parameters:
            accessor: HelixDataAccessorinstance: StringresourceGroup: StringsessionID: Stringpartition: String
        Java modifiers:
             static

        """
        # Builder
        keyBuilder = accessor.keyBuilder()
        # List<ZNRecord>
        instances = HelixProperty.convertToList(accessor.getChildValues(keyBuilder.instanceConfigs()))
        # List<ZNRecord>
        partitionRecords = []
        for znRecord in instances: # String
            instanceName = znRecord.getId()
            if not (instanceName == instance):
                continue

            # List<String>
            sessions = accessor.getChildNames(keyBuilder.sessions(instanceName))
            for session in sessions:
                if sessionID != None and not (session == sessionID):
                    continue

                # List<String>
                resourceGroups = accessor.getChildNames(keyBuilder.stateTransitionStatus(instanceName, session))
                for resourceGroupName in resourceGroups:
                    if not (resourceGroupName == resourceGroup):
                        continue

                    # List<String>
                    partitionStrings = accessor.getChildNames(keyBuilder.stateTransitionStatus(instanceName, session, resourceGroupName))
                    for partitionString in partitionStrings: # ZNRecord
                        partitionRecord = accessor.getProperty(keyBuilder.stateTransitionStatus(instanceName, session, resourceGroupName, partitionString)).getRecord()
                        if not (partitionString == partition):
                            continue

                        partitionRecords.append(partitionRecord)




        return StatusUpdateContents(self.getSortedTransitions(partitionRecords), self.getTaskMessages(partitionRecords))


    def getTransitions(self):
        """
        Returns List<Transition>


        """
        return self._transitions


    def getTaskMessages(self):
        """
        Returns Map<String, TaskStatus>


        """
        return self._taskMessages


    def getSortedTransitions(self, partitionRecords):
        """
        Returns List<Transition>
        Parameters:
            partitionRecords: List<ZNRecord>
        Java modifiers:
             private static

        """
        # List<Transition>
        transitions = []
        for partition in partitionRecords: # Map<String, Map<String, String>>
            mapFields = partition.getMapFields()
            for key in mapFields.keys():
                if key.startsWith("MESSAGE"):
                    # Map<String, String>
                    m = mapFields.get(key)
                    # long
                    createTimeStamp = 0
                    try:
                        createTimeStamp = long(m.get("CREATE_TIMESTAMP"))
                    except Exception, e: pass

                    transitions.add(Transition(m.get("MSG_ID"), createTimeStamp, m.get("FROM_STATE"), m.get("TO_STATE")))



        transitions.sort()
        return transitions


    def getTaskMessages(self, partitionRecords):
        """
        Returns Map<String, TaskStatus>
        Parameters:
            partitionRecords: List<ZNRecord>
        Java modifiers:
             private static

        """
        # Map<String, TaskStatus>
        taskMessages = {}
        for partition in partitionRecords: # Map<String, Map<String, String>>
            mapFields = partition.getMapFields()
            for key in mapFields.keys():
                if key.__contains__("STATE_TRANSITION"):
                    # Map<String, String>
                    m = mapFields.get(key)
                    # String
                    id = m.get("MSG_ID")
                    # String
                    statusString = m.get("AdditionalInfo")
                    # TaskStatus
                    status = TaskStatus.UNKNOWN
                    if statusString.contains("scheduled"):
                        status = TaskStatus.SCHEDULED
                    elif statusString.contains("invoking"):
                        status = TaskStatus.INVOKING
                    elif statusString.contains("completed"):
                        status = TaskStatus.COMPLETED
                    taskMessages.put(id, status)

        return taskMessages


class StatusUpdateUtil:

    """
    Java modifiers:
         static
    Type:
        Logger
    """
    _logger = get_logger(__name__)


    def __init__(self):
        self._recordedMessages = None

    def createEmptyStatusUpdateRecord(self, id):
        """
        Returns ZNRecord
        Parameters:
            id: String


        """
        return ZNRecord(id)


    def createMessageLogRecord(self, message):
        """
        Returns ZNRecord
        Parameters:
            message: Message


        """
        # ZNRecord
        result = ZNRecord(self.getStatusUpdateRecordName(message))
        # String
        mapFieldKey = "MESSAGE " + message.getMsgId()
#        result.setMapField(mapFieldKey, TreeMap<String, String>())
        result.setMapField(mapFieldKey, {})
        for simpleFieldKey in message.getRecord().getSimpleFields().keys():
            result.getMapField(mapFieldKey).__setitem__(simpleFieldKey, message.getRecord().getSimpleField(simpleFieldKey))

        if message.getResultMap() != None: 
            result.setMapField("MessageResult", message.getResultMap())

        return result




    def createMessageStatusUpdateRecord(self, message, level, classInfo, additionalInfo):
        """
        Returns ZNRecord
        Parameters:
            message: Messagelevel: LevelclassInfo: ClassadditionalInfo: String


        """
        # ZNRecord
        result = self.createEmptyStatusUpdateRecord(self.getStatusUpdateRecordName(message))
        # Map<String, String>
        contentMap = {}
        contentMap.__setitem__("Message state", str(message.getMsgState()))
        contentMap.__setitem__("AdditionalInfo", additionalInfo)
        contentMap.__setitem__("Class", str(classInfo))
        contentMap.__setitem__("MSG_ID", message.getMsgId())
        # DateFormat
#        formatter = SimpleDateFormat("yyyyMMdd-HHmmss.SSSSSS")
        # String
#        time = formatter.format(Date())
        curtime = time.strftime("%Y%m%d-%H%M%S")
        # String
        id = "%4s %26s " % (Level.toString(level), curtime) + self.getRecordIdForMessage(message)
        result.setMapField(id, contentMap)
        return result


    def getRecordIdForMessage(self, message):
        """
        Returns String
        Parameters:
            message: Message


        """
        if (message.getMsgType() == MessageType.STATE_TRANSITION): 
            return message.getPartitionName() + " Trans:" + message.getFromState().charAt(0) + "->" + message.getToState().charAt(0) + "  " + str(uuid.uuid4())
        else:
            return message.getMsgType() + " " + str(uuid.uuid4())



    def logMessageStatusUpdateRecord(self, message, level, classInfo, additionalInfo, accessor):
        """
        Returns void
        Parameters:
            message: Messagelevel: LevelclassInfo: ClassadditionalInfo: Stringaccessor: HelixDataAccessor


        """
        # TODO: enable the catch
#        try:
            # ZNRecord
        record = self.createMessageStatusUpdateRecord(message, level, classInfo, additionalInfo)
        self.publishStatusUpdateRecord(record, message, level, accessor)
#        except Exception, e:
#            self._logger.error("Exception while logging status update"+ str(e))



    def logError(self, message, classInfo, additionalInfo, accessor):
        """
        Returns void
        Parameters:
            message: MessageclassInfo: ClassadditionalInfo: Stringaccessor: HelixDataAccessor


        """
        self.logMessageStatusUpdateRecord(message, Level.HELIX_ERROR, classInfo, additionalInfo, accessor)


    def logError(self, message, classInfo, e, additionalInfo, accessor):
        """
        Returns void
        Parameters:
            message: MessageclassInfo: Classe: ExceptionadditionalInfo: Stringaccessor: HelixDataAccessor


        """
        # TODO. get the stack
#        # StringWriter
#        sw = StringWriter()
#        # PrintWriter
#        pw = PrintWriter(sw)
#        e.printStackTrace(pw)
#        self.logMessageStatusUpdateRecord(message, Level.HELIX_ERROR, classInfo, additionalInfo + sw.toString(), accessor)
        self.logMessageStatusUpdateRecord(message, Level.HELIX_ERROR, classInfo, additionalInfo + "stack", accessor)


    def logInfo(self, message, classInfo, additionalInfo, accessor):
        """
        Returns void
        Parameters:
            message: MessageclassInfo: ClassadditionalInfo: Stringaccessor: HelixDataAccessor


        """
        self.logMessageStatusUpdateRecord(message, Level.HELIX_INFO, classInfo, additionalInfo, accessor)


    def logWarning(self, message, classInfo, additionalInfo, accessor):
        """
        Returns void
        Parameters:
            message: MessageclassInfo: ClassadditionalInfo: Stringaccessor: HelixDataAccessor


        """
        self.logMessageStatusUpdateRecord(message, Level.HELIX_WARNING, classInfo, additionalInfo, accessor)


    def publishStatusUpdateRecord(self, record, message, level, accessor):
        """
        Returns void
        Parameters:
            record: ZNRecordmessage: Messagelevel: Levelaccessor: HelixDataAccessor


        """
        # String
        instanceName = message.getTgtName()
        # String
        statusUpdateSubPath = self.getStatusUpdateSubPath(message)
        # String
        statusUpdateKey = self.getStatusUpdateKey(message)
        # String
        sessionId = message.getExecutionSessionId()
        if sessionId == None: 
            sessionId = message.getTgtSessionId()

        if sessionId == None: 
            sessionId = "*"

        # Builder
        keyBuilder = accessor.keyBuilder()
        if self._recordedMessages and  self._recordedMessages.__contains__(message.getMsgId()):
            if instanceName.upper() == "Controller".upper():
                accessor.updateProperty(keyBuilder.controllerTaskStatus(statusUpdateSubPath, statusUpdateKey), StatusUpdate(self.createMessageLogRecord(message)))
            else:
                # PropertyKey
                propertyKey = keyBuilder.stateTransitionStatus(instanceName, sessionId, statusUpdateSubPath, statusUpdateKey)
                # ZNRecord
                statusUpdateRecord = self.createMessageLogRecord(message)
                self._logger.info("StatusUpdate path:" + propertyKey.getPath() + ", updates:" + str(statusUpdateRecord))
                accessor.updateProperty(propertyKey, StatusUpdate(statusUpdateRecord))

            self._recordedMessages.__setitem__(message.getMsgId(), message.getMsgId())

        if instanceName.upper() == "Controller".upper():
            accessor.updateProperty(keyBuilder.controllerTaskStatus(statusUpdateSubPath, statusUpdateKey), StatusUpdate(record))
        else:
            # PropertyKey
            propertyKey = keyBuilder.stateTransitionStatus(instanceName, sessionId, statusUpdateSubPath, statusUpdateKey)
            self._logger.info("StatusUpdate path:" + propertyKey.getPath() + ", updates:" + str(record))
            accessor.updateProperty(propertyKey, StatusUpdate(record))

        if Level.HELIX_ERROR == level:
            self.publishErrorRecord(record, message, accessor)



    def getStatusUpdateKey(self, message):
        """
        Returns String
        Parameters:
            message: Message
        Java modifiers:
             private

        """
        if message.getMsgType().upper() == MessageType.toString(MessageType.STATE_TRANSITION).upper():
            return message.getPartitionName()

        return message.getMsgId()


    def getStatusUpdateSubPath(self, message):
        """
        Returns String
        Parameters:
            message: Message


        """
        if message.getMsgType().upper() == MessageType.toString(MessageType.STATE_TRANSITION).upper():
            return message.getResourceName()
        else:
            return message.getMsgType()



    def getStatusUpdateRecordName(self, message):
        """
        Returns String
        Parameters:
            message: Message


        """
        if message.getMsgType().upper() == MessageType.toString(MessageType.STATE_TRANSITION).upper():
            return message.getTgtSessionId() + "__" + message.getResourceName()

        return message.getMsgId()


    def publishErrorRecord(self, record, message, accessor):
        """
        Returns void
        Parameters:
            record: ZNRecordmessage: Messageaccessor: HelixDataAccessor


        """
        # String
        instanceName = message.getTgtName()
        # String
        statusUpdateSubPath = self.getStatusUpdateSubPath(message)
        # String
        statusUpdateKey = self.getStatusUpdateKey(message)
        # String
        sessionId = message.getExecutionSessionId()
        if sessionId == None: 
            sessionId = message.getTgtSessionId()

        if sessionId == None: 
            sessionId = "*"

        # Builder
        keyBuilder = accessor.keyBuilder()
        if instanceName.lower() == "controller":
            accessor.setProperty(keyBuilder.controllerTaskError(statusUpdateSubPath, statusUpdateKey), Error(record))
        else:
            accessor.updateProperty(keyBuilder.stateTransitionError(instanceName, sessionId, statusUpdateSubPath, statusUpdateKey), Error(record))




