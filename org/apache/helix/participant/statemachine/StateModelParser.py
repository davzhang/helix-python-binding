# package org.apache.helix.participant.statemachine
#from org.apache.helix.participant.statemachine import *
#from java.lang.reflect import Method
#from java.util import Arrays
from org.apache.helix.NotificationContext import NotificationContext
from org.apache.helix.model.Message import Message
from org.apache.helix.participant.statemachine import StateModel


class StateModelParser:

    def getMethodForTransition(self, clazz, fromState, toState):
        """
        Returns Method
        Parameters:
            clazz: Class<? extends StateModel>fromState: StringtoState: StringparamTypes: Class<?>[]


        """
        # Method
#        method = getMethodForTransitionUsingAnnotation(clazz, fromState, toState, paramTypes)
#        if method == None:
#            method = getMethodForTransitionByConvention(clazz, fromState, toState, paramTypes)
        methodName = "onBecome" + toState.lower().capitalize() + "From" + fromState.lower().capitalize()
        return getattr(clazz, methodName)

#        return method


#    def getMethodForTransitionByConvention(self, clazz, fromState, toState, paramTypes):
#        """
#        Returns Method
#        Parameters:
#            clazz: Class<? extends StateModel>fromState: StringtoState: StringparamTypes: Class<?>[]
#
#
#        """
#        # Method
#        methodToInvoke = None
#        # String
#        methodName = "onBecome" + toState + "From" + fromState
#        if (fromState == "*"):
#            methodName = "onBecome" + toState
#
#        # Method[]
#        methods = clazz.getMethods()
#        for # Method
#        method = None
#         in methods) if method.getName().equalsIgnoreCase(methodName):
#                # Class<?>[]
#                parameterTypes = method.getParameterTypes()
#                if parameterTypes.length == 2 && (parameterTypes[0] == Message.class) && (parameterTypes[1] == NotificationContext.class):
#                    methodToInvoke = method
#                    break
#
#
#
#        return methodToInvoke
#
#
#    def getMethodForTransitionUsingAnnotation(self, clazz, fromState, toState, paramTypes):
#        """
#        Returns Method
#        Parameters:
#            clazz: Class<? extends StateModel>fromState: StringtoState: StringparamTypes: Class<?>[]
#
#
#        """
#        # StateModelInfo
#        stateModelInfo = clazz.getAnnotation(StateModelInfo.class)
#        # Method
#        methodToInvoke = None
#        if stateModelInfo != None:
#            # Method[]
#            methods = clazz.getMethods()
#            if methods != None:
#                for # Method
#                method = None
#                 in methods) # Transition
#                    annotation = method.getAnnotation(Transition.class)
#                    if annotation != None:
#                        # boolean
#                        matchesFrom = annotation.from().equalsIgnoreCase(fromState)
#                        # boolean
#                        matchesTo = annotation.to().equalsIgnoreCase(toState)
#                        # boolean
#                        matchesParamTypes = (Arrays == paramTypes)
#                        if matchesFrom && matchesTo && matchesParamTypes:
#                            methodToInvoke = method
#                            break
#
#
#
#
#
#        return methodToInvoke
#

    def getInitialState(self, clazz):
        """
        Returns String
        Parameters:
            clazz: Class<? extends StateModel>


        """
        # StateModelInfo
#        stateModelInfo = clazz.getAnnotation(StateModelInfo.class)
#        if stateModelInfo != None:
#            return stateModelInfo.initialState()
#        else:
#            return StateModel.DEFAULT_INITIAL_STATE
        return StateModel.DEFAULT_INITIAL_STATE




