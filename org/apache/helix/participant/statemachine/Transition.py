# package org.apache.helix.participant.statemachine
#from org.apache.helix.participant.statemachine import *
#from java.lang.annotation import Retention
#from java.lang.annotation import RetentionPolicy

# Annotation: @Retention(RetentionPolicy.RUNTIME)


@interface Transition {


    String from()


    String to()
}

