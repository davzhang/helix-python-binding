# package org.apache.helix.agent
#from org.apache.helix.agent import *
#from java.util import HashMap
#from java.util import Map

class CommandAttributeProperty(object):
    commandMap = {}

    def __init__(self, name):
        self._name = name

    def getName(self):
        return self._name

    def getCommandAttributeByName(name):
        return name in CommandAttributeProperty.commandMap and CommandAttributeProperty.commandMap[name] or None

enumDict = {"COMMAND":"command","WORKING_DIR":"command.workingDir","TIMEOUT":"command.timeout", "PID_FILE":"command.pidFile", "NOP":"nop"}
class CommandAttribute(object): pass

# dynamically add the attributes
for key in enumDict:
    commandAttribute = CommandAttributeProperty(enumDict[key])
    setattr(CommandAttribute, key, commandAttribute)
    CommandAttributeProperty.commandMap[key] = commandAttribute.getName()



