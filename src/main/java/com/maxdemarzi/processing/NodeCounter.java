package com.maxdemarzi.processing;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

/**
 * @author mh
 * @since 28.03.15
 */
public class NodeCounter {
    public int getNodeCount(GraphDatabaseService db) {
        Result result = db.execute( "MATCH (n) RETURN max(id(n)) AS maxId" );
        return ((Number) result.next().get( "maxId" )).intValue() + 1;

    }
    public int getRelationshipCount(GraphDatabaseService db) {
        Result result = db.execute( "MATCH ()-[r]->() RETURN max(id(r)) AS maxId" );
        return ((Number) result.next().get( "maxId" )).intValue() + 1;
    }
}
