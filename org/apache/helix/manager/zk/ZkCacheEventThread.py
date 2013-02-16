# package org.apache.helix.manager.zk
#from org.apache.helix.manager.zk import *
#from java.util.concurrent import BlockingQueue
#from java.util.concurrent import LinkedBlockingQueue
#from java.util.concurrent.atomic import AtomicInteger
#from org.I0Itec.zkclient.exception import ZkInterruptedException
#from org.apache.log4j import Logger


class ZkCacheEventThread(Thread):

    """
    Java modifiers:
         private final static
    Type:
        Logger
    """
    LOG = Logger.getLogger(ZkCacheEventThread.class)



    """
    Java modifiers:
         private static
    Type:
        AtomicInteger
    """
    _eventId = AtomicInteger(0)

    Java modifiers:
         abstract static
    class ZkCacheEvent:



        """

        Parameters:
            String description
        """
        def __init__(self, description):
            self._description = description


        def run(self):
            """
            Returns void
            Java modifiers:
                 abstract

            Throws: 
                Exception
            """
            pass


        def toString(self):
            """
            Returns String
            @Override


            """
            return "ZkCacheEvent[" + _description + "]"



    """

    Parameters:
        String name
    """
    def __init__(self, name):
        setDaemon(True)
        setName("ZkCache-EventThread-" + getId() + "-" + name)


    def run(self):
        """
        Returns void
        @Override


        """
        LOG.info("Starting ZkCache event thread.")
        try:
            while (not isInterrupted():
                # ZkCacheEvent
                zkEvent = _events.take()
                # int
                eventId = _eventId.incrementAndGet()
                LOG.debug("Delivering event #" + str(eventId)+ " " + str(zkEvent))
                try:
                    zkEvent.run()
                except InterruptedException, e:
                    interrupt()
                except ZkInterruptedException, e:
                    interrupt()
                except Throwable, e:
                    LOG.error("Error handling event " + str(zkEvent)+ str(e))

                LOG.debug("Delivering event #" + str(eventId)+ " done")

        except InterruptedException, e:
            LOG.info("Terminate ZkClient event thread.")



    def send(self, event):
        """
        Returns void
        Parameters:
            event: ZkCacheEvent


        """
        if not isInterrupted(): 
            LOG.debug("New event: " + str(event))
            _events.add(event)




