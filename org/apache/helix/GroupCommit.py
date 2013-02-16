# package org.apache.helix
#from org.apache.helix import *
#from java.util import ArrayList
#from java.util import Iterator
#from java.util.concurrent import ConcurrentLinkedQueue
#from java.util.concurrent.atomic import AtomicBoolean
#from java.util.concurrent.atomic import AtomicReference
#from org.I0Itec.zkclient.exception import ZkNoNodeException
#from org.apache.log4j import Logger

from org.apache.helix.util.logger import get_logger
from Queue import Queue

class GroupCommit:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
#    LOG = Logger.getLogger(GroupCommit.class)
    LOG = get_logger(__name__)

#    Java modifiers:
#         private static
#    class Queue:

    class Entry:

        """

        Parameters:
            String key
            ZNRecord record
        """
        def __init__(self, key, record):
            self._key = key
            self._record = record


    """

    """
    def __init__(self):

        self._queues = []
        for i in range(100): self._queues.append(Queue())

    def getQueue(self, key):
        """
        Returns Queue
        Parameters:
            key: String
        Java modifiers:
             private

        """
        return self._queues[hash(key)  % self._queues.__len__()]

    def commit(self, accessor, options, key, record):
        accessor.set(key, record, options)

    # comment this out, simple commit for now
#    def commit(self, accessor, options, key, record):
#        """
#        Returns boolean
#        Parameters:
#            accessor: BaseDataAccessor<ZNRecord>options: intkey: Stringrecord: ZNRecord
#
#
#        """
#        # Queue
#        queue = self.getQueue(key)
#        # Entry
#        entry = Entry(key, record)
#        queue._pending.add(entry)
#        while (not entry._sent.get():
#            if queue._running.compareAndSet(None, Thread.currentThread()):
#                # ArrayList<Entry>
#                processed = ArrayList<Entry>()
#                try:
#                    if queue._pending.peek() == None:
#                        return True
#                    # Entry
#                    first = queue._pending.poll()
#                    processed.add(first)
#                    # String
#                    mergedKey = first._key
#                    # ZNRecord
#                    merged = None
#                    try:
#                        merged = accessor.get(mergedKey, None, options)
#                    except ZkNoNodeException, e:
#
#                    if merged == None:
#                            merged = ZNRecord(first._record)
#
#                    else:
#                        merged.merge(first._record)
#
#                    # Iterator<Entry>
#                    it = queue._pending.iterator()
#                    while (it.hasNext():
#                        # Entry
#                        ent = it.next()
#                        if not (ent._key == mergedKey):
#                            continue
#                        processed.add(ent)
#                        merged.merge(ent._record)
#                        it.remove()
#
#                    accessor.set(mergedKey, merged, options)
#                final:
#                        queue._running.set(None)
#                        for # Entry
#                        e = None
#                         in processed) synchronized (e) e._sent.set(True)
#                                e.notify()
#
#
#
#            else:
#                synchronized (entry) try:
#                        entry.wait(10)
#                    except InterruptedException, e:
#                        e.printStackTrace()
#                        return False
#
#
#
#
#        return True



