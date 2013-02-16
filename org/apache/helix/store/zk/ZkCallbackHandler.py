# package org.apache.helix.store.zk
#from org.apache.helix.store.zk import *
#from java.util import List
#from org.I0Itec.zkclient import IZkChildListener
#from org.I0Itec.zkclient import IZkDataListener
#from org.apache.log4j import Logger
from org.apache.helix.manager.zk.ZkClient import ZkClient
from org.apache.helix.store.PropertyChangeListener import PropertyChangeListener


# Parameterized type: <T>
class ZkCallbackHandler(IZkChildListener, IZkDataListener):

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(ZkCallbackHandler.class)









    """

    Parameters:
        ZkClient client
        ZKPropertyStore<T> store
        String prefix
        PropertyChangeListener<T> listener
    """
    def __init__(self, client, store, prefix, listener):
        self._zkClient = client
        self._store = store
        self._prefix = prefix
        self._listener = listener


    def handleDataChange(self, path, data):
        """
        Returns void
        Parameters:
            path: Stringdata: Object
        @Override


        Throws: 
            Exception
        """
        LOG.debug("Data changed @ " + str(path)+ " to " + str(data))
        # String
        key = _store.getRelativePath(path)
        _listener.onPropertyChange(key)


    def handleDataDeleted(self, dataPath):
        """
        Returns void
        Parameters:
            dataPath: String
        @Override


        Throws: 
            Exception
        """
        LOG.debug("Data deleted @ " + str(dataPath))


    def handleChildChange(self, path, currentChilds):
        """
        Returns void
        Parameters:
            path: StringcurrentChilds: List<String>
        @Override


        Throws: 
            Exception
        """
        LOG.debug("childs changed @ " + str(path)+ " to " + str(currentChilds))
        if currentChilds == None: 
            return
        else:
            # String
            key = _store.getRelativePath(path)
            _listener.onPropertyChange(key)
            for # String
            child = None
             in currentChilds) # String
                childPath = java2python_runtime.ternary(path.endsWith("/"), path + child, path + "/" + child)
                _zkClient.subscribeDataChanges(childPath, self)
                _zkClient.subscribeChildChanges(childPath, self)
                handleChildChange(childPath, _zkClient.getChildren(childPath))





