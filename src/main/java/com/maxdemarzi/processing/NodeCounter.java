package com.maxdemarzi.processing;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.NeoStore;

/**
 * @author mh
 * @since 28.03.15
 */
public class NodeCounter {
    public int getNodeCount(GraphDatabaseService db) {
        NeoStore neoStore = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(NeoStore.class);
        return (int) neoStore.getNodeStore().getHighId();
    }
}
