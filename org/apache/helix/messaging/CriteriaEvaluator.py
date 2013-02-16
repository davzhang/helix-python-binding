# package org.apache.helix.messaging
#from org.apache.helix.messaging import *
#from java.util import ArrayList
#from java.util import HashMap
#from java.util import List
#from java.util import Map
#from org.apache.log4j import Logger
from org.apache.helix.HelixManager import HelixManager
from org.apache.helix.Criteria import Criteria
#from org.apache.helix.josql.ClusterJosqlQueryProcessor import ClusterJosqlQueryProcessor
#from org.apache.helix.josql.ZNRecordRow import ZNRecordRow

from org.apache.helix.util.logger import get_logger
#from org.apache.helix.util.UserExceptions import IllegalArgumentException
from org.apache.helix.util.misc import enum, ternary


class CriteriaEvaluator:

    """
    Java modifiers:
         private static
    Type:
        Logger
    """
    logger = get_logger(__name__)

    def evaluateCriteria(self, recipientCriteria, manager):
        """
        Returns List<Map<String, String>>
        Parameters:
            recipientCriteria: Criteriamanager: HelixManager


        """
        # List<Map<String, String>>
        selected = []
        #        # TODO: Skip the query for now
#        selected = ArrayList<Map<String, String>>()
#        # String
#        queryFields = ternary(not (recipientCriteria.getInstanceName() == ""), " " + ZNRecordRow.MAP_SUBKEY, " ''") + "," + (ternary(not (recipientCriteria.getResource() == ""), " " + ZNRecordRow.ZNRECORD_ID, " ''")) + "," + (ternary(not (recipientCriteria.getPartition() == ""), " " + ZNRecordRow.MAP_KEY, " ''")) + "," + (ternary(not (recipientCriteria.getPartitionState() == ""), " " + ZNRecordRow.MAP_VALUE, " '' "))
#        # String
#        matchCondition = ZNRecordRow.MAP_SUBKEY + " LIKE '" + (ternary(not (recipientCriteria.getInstanceName() == ""), (recipientCriteria.getInstanceName() + "'"), "%' ")) + " AND " + ZNRecordRow.ZNRECORD_ID + " LIKE '" + (ternary(not (recipientCriteria.getResource() == ""), (recipientCriteria.getResource() + "'"), "%' ")) + " AND " + ZNRecordRow.MAP_KEY + " LIKE '" + (ternary(not (recipientCriteria.getPartition() == ""), (recipientCriteria.getPartition() + "'"), "%' ")) + " AND " + ZNRecordRow.MAP_VALUE + " LIKE '" + (ternary(not (recipientCriteria.getPartitionState() == ""), (recipientCriteria.getPartitionState() + "'"), "%' ")) + " AND " + ZNRecordRow.MAP_SUBKEY + " IN ((SELECT [*]id FROM :LIVEINSTANCES))"
#        # String
#        queryTarget = recipientCriteria.getDataSource().toString() + ClusterJosqlQueryProcessor.FLATTABLE
#        # String
#        josql = "SELECT DISTINCT " + queryFields + " FROM " + queryTarget + " WHERE " + matchCondition
#        # ClusterJosqlQueryProcessor
#        # TODO: Skip the query for now
##        p = ClusterJosqlQueryProcessor(manager)
#        # List<Object>
#        result = []
##        result = ArrayList<Object>()
#        try:
#            self.logger.info("JOSQL query: " + josql)
#            # TODO: Skip the query for now
##            result = p.runJoSqlQuery(josql, None, None)
#        except Exception, e:
#            self.logger.error(""+ str(e))
#            return selected
#
#        for o in result: # Map<String, String>
##            resultRow = HashMap<String, String>()
#            resultRow = {}
#            # List<Object>
#            row = o
##            row = (List<Object>) o
#            resultRow.__setitem__("instanceName", (row[0]))
#            resultRow.__setitem__("resourceName", (row[1]))
#            resultRow.__setitem__("partitionName",(row[2]))
#            resultRow.__setitem__("partitionState", (row[3]))
#            selected.append(resultRow)
#
#        self.logger.info("JOSQL query return " + selected.size() + " rows")
        return selected



