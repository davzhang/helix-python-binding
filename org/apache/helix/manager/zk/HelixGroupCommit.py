# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util import ArrayList
#from java.util import Iterator
#from java.util.concurrent import ConcurrentLinkedQueue
#from java.util.concurrent.atomic import AtomicBoolean
#from java.util.concurrent.atomic import AtomicReference
#from org.I0Itec.zkclient import DataUpdater
#from org.I0Itec.zkclient.exception import ZkBadVersionException
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger
#from org.apache.zookeeper.data import Stat


# Parameterized type: <T>
class HelixGroupCommit:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    LOG = Logger.getLogger(HelixGroupCommit.class)

    Java modifiers:
         private static
    # Parameterized type: <T>
    class Queue:






    Java modifiers:
         private static
    # Parameterized type: <T>
    class Entry:







        """

        Parameters:
            String key
            DataUpdater<T> updater
        """
        def __init__(self, key, updater):
            self._key = key
            self._updater = updater





    """

    """
    def __init__(self):
        for (# int
        i = 0; i < _queues.length; ++i) _queues[i] = Queue<T>()



    def getQueue(self, key):
        """
        Returns Queue<T>
        Parameters:
            key: String
        Java modifiers:
             private

        """
        return _queues[(key.hashCode() & Integer.MAX_VALUE) % _queues.length]


    def commit(self, accessor, options, key, updater):
        """
        Returns boolean
        Parameters:
            accessor: ZkBaseDataAccessor<T>options: intkey: Stringupdater: DataUpdater<T>


        """
        # Queue<T>
        queue = getQueue(key)
        # Entry<T>
        entry = Entry<T>(key, updater)
        queue._pending.add(entry)
        while (not entry._sent.get():
            if queue._running.compareAndSet(None, Thread.currentThread()): 
                # ArrayList<Entry<T>>
                processed = ArrayList<Entry<T>>()
                try:
                    # Entry<T>
                    first = queue._pending.peek()
                    if first == None: 
                        return True

                    # String
                    mergedKey = first._key
                    # boolean
                    retry = None
                    
                    do retry = False
                        try:
                            # T
                            merged = None
                            # Stat
                            readStat = Stat()
                            try:
                                merged = accessor.get(mergedKey, readStat, options)
                            except ZkNoNodeException, e:

                            # Iterator<Entry<T>>
                            it = processed.iterator()
                            while (it.hasNext():
                                # Entry<T>
                                ent = it.next()
                                if not (ent._key == mergedKey): 
                                    continue

                                merged = ent._updater.update(merged)

                            it = queue._pending.iterator()
                            while (it.hasNext():
                                # Entry<T>
                                ent = it.next()
                                if not (ent._key == mergedKey): 
                                    continue

                                processed.add(ent)
                                merged = ent._updater.update(merged)
                                it.remove()

                            accessor.set(mergedKey, merged, None, None, readStat.getVersion(), options)
                        except ZkBadVersionException, e:
                            retry = True

                     while (retry)
                final:
                        queue._running.set(None)
                        for # Entry<T>
                        e = None
                         in processed) synchronized (e) e._sent.set(True)
                                e.notify()



            else:
                synchronized (entry) try:
                        entry.wait(10)
                    except InterruptedException, e:
                        e.printStackTrace()
                        return False




        return True



