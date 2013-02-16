# package org.apache.helix.model
#from org.apache.helix.model import *
#from java.util import ArrayList
#from java.util import HashMap
#from java.util import HashSet
#from java.util import List
#from java.util import Map
#from java.util import Set
#from java.util import TreeMap
#from org.apache.log4j import Logger
from org.apache.helix.HelixProperty import HelixProperty
from org.apache.helix.ZNRecord import ZNRecord
from org.apache.helix.model.Message import MessageType
from org.apache.helix.util.logger import get_logger
from org.apache.helix.util.misc import enum

ConstraintAttribute=enum('STATE', 'MESSAGE_TYPE', 'TRANSITION', 'RESOURCE', 'INSTANCE', 'CONSTRAINT_VALUE')

ConstraintValue=enum('ANY')

ConstraintTypeenum=enum('STATE_CONSTRAINT', 'MESSAGE_CONSTRAINT')

class ClusterConstraints(HelixProperty):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = get_logger(__name__)



    class ConstraintItem:

        """

        Parameters:
            Map<String, String> attributes
        """
        def __init__(self, attributes):
#            self._attributes = TreeMap<ConstraintAttribute, String>()
            self._attributes = {}
            self._constraintValue = None
            if attributes != None:
                for key in attributes.keys:
                    try:
                        # ConstraintAttribute
                        attr = getattr(ConstraintAttribute,key)
                        if attr == ConstraintAttribute.CONSTRAINT_VALUE: 
                            # String
                            value = attributes.get(key)
                            try:
                                ConstraintValue.valueOf(value)
                            except Exception, e:
                                try:
                                    int(value)
                                except ValueError, ne:
                                    self.LOG.error("Invalid constraintValue " + str(key)+ ":" + str(value))
                                    continue


                            self._constraintValue = attributes.get(key)
                        else:
                            self._attributes.__setitem__(attr, attributes.get(key))

                    except Exception, e:
                        self.LOG.error("Invalid constraintAttribute " + str(key)+ ":" + str(attributes.get(key)))
                        continue





        def match(self, attributes):
            """
            Returns boolean
            Parameters:
                attributes: Map<ConstraintAttribute, String>


            """
            for key in self._attributes.keys():
                if not attributes.__contains__(key):
                    return False

                if not attributes.get(key).matches(self._attributes.get(key)):
                    return False


            return True


        def filter(self, attributes):
            """
            Returns Map<ConstraintAttribute, String>
            Parameters:
                attributes: Map<ConstraintAttribute, String>


            """
            # Map<ConstraintAttribute, String>
            ret = {}
            for key in self._attributes.keys:
                ret.__setitem__(key, attributes.get(key))

            return ret


        def getConstraintValue(self):
            """
            Returns String


            """
            return self._constraintValue


        def getAttributes(self):
            """
            Returns Map<ConstraintAttribute, String>


            """
            return self._attributes


        def toString(self):
            """
            Returns String
            @Override


            """
            # StringBuffer
#            sb = StringBuffer()
#            sb.append(_attributes + ":" + _constraintValue)
#            return sb.toString()
            return str(self._attributes) + ":" + str(self._constraintValue)





    """

    Parameters:
        ZNRecord record
    """
    def __init__(self, record):
        super(record)
        for key in self._record.getMapFields().keys(): # ConstraintItem
            item = self.ConstraintItem(self._record.getMapField(key))
            if item.getAttributes().size() > 0 and item.getConstraintValue() != None:
                self._constraints.add(item)
            else:
                self.LOG.error("Invalid constraint " + str(key)+ ":" + str(self._record.getMapField(key)))




    def match(self, attributes):
        """
        Returns Set<ConstraintItem>
        Parameters:
            attributes: Map<ConstraintAttribute, String>


        """
        # Set<ConstraintItem>
#        matches = HashSet<ConstraintItem>()
        matches = set()
        for item in self._constraints:
            if item.match(attributes):
                matches.add(item)


        return matches


    def toConstraintAttributes(msg):
        """
        Returns Map<ConstraintAttribute, String>
        Parameters:
            msg: Message
        Java modifiers:
             static

        """
        # Map<ConstraintAttribute, String>
        attributes = {}
#        attributes = TreeMap<ConstraintAttribute, String>()
        # String
        msgType = msg.getMsgType()
        attributes.put(ConstraintAttribute.MESSAGE_TYPE, msgType)
        if (MessageType.STATE_TRANSITION.toString() == msgType): 
            if msg.getFromState() != None and msg.getToState() != None:
                attributes.put(ConstraintAttribute.TRANSITION, msg.getFromState() + "-" + msg.getToState())

            if msg.getResourceName() != None: 
                attributes.put(ConstraintAttribute.RESOURCE, msg.getResourceName())

            if msg.getTgtName() != None: 
                attributes.put(ConstraintAttribute.INSTANCE, msg.getTgtName())


        return attributes


    def isValid(self):
        """
        Returns boolean
        @Override


        """
        return True



