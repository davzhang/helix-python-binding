# package org.apache.helix
#from org.apache.helix import *

#def enum(**enums):
#    return type('Enum', (), enums)


#enum InstanceType {
#
#    CONTROLLER, PARTICIPANT, SPECTATOR, CONTROLLER_PARTICIPANT, ADMINISTRATOR
#}

from org.apache.helix.util.misc import enum
InstanceType = enum(
    'CONTROLLER', 'PARTICIPANT', 'SPECTATOR', 'CONTROLLER_PARTICIPANT', 'ADMINISTRATOR'
)

