package com.maxdemarzi.processing.unionfind;

import it.unimi.dsi.fastutil.longs.*;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.tooling.GlobalGraphOperations;

/*
    Weighted quick-union with path compression
    See https://www.cs.princeton.edu/~rs/AlgsDS07/01UnionFind.pdf
 */

public class UnionFindMapStorage implements UnionFind {
    private final GraphDatabaseService db;
    private final int nodes;
    private Long2IntOpenHashMap rankMap;
    private Long2LongMap rootMap;

    public UnionFindMapStorage(GraphDatabaseService db) {
        this.db = db;
        this.rootMap = new Long2LongOpenHashMap();
        this.rankMap = new Long2IntOpenHashMap();
        NeoStoreProvider neoStoreProvider = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(NeoStoreProvider.class);
        this.nodes = (int) neoStoreProvider.evaluate().getNodeStore().getHighId();
    }

    @Override
    public void compute(String label, String type, int iterations) {
        compute(label, type);
    }

    @Override
    public void compute(String label, String type) {
        RelationshipType relationshipType = DynamicRelationshipType.withName(type);

        try ( Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodes = db.findNodes(DynamicLabel.label(label));
            while (nodes.hasNext()) {
                long nodeId = nodes.next().getId();
                rootMap.put(nodeId, nodeId);
                rankMap.put(nodeId, 0);
            }

            for( Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                if (relationship.isType(relationshipType)) {

                    long x = relationship.getStartNode().getId();
                    long y = relationship.getEndNode().getId();
                    if (x == y) { continue; }

                    x = rootMap.get(relationship.getStartNode().getId());
                    y = rootMap.get(relationship.getEndNode().getId());

                    while (x != rootMap.get(x)) {
                        rootMap.put(x, rootMap.get(rootMap.get(x)));
                        x = rootMap.get(x);
                    }
                    while (y != rootMap.get(y)) {
                        rootMap.put(y, rootMap.get(rootMap.get(y)));
                        y = rootMap.get(y);
                    }

                    if (x != y) {
                        if ( rankMap.get(x) > rankMap.get(y)) {
                            rootMap.put(y, x);
                        } else if (rankMap.get(x) < rankMap.get(y)) {
                            rootMap.put(x, y);
                        } else {
                            rootMap.put(y, x);
                            rankMap.put(x, rankMap.get(x) + 1);
                        }
                    }
                }
            }
        }
    }

    @Override
    public double getResult(long node) {
        return rootMap != null ? rootMap.get(node) : -1;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    };
}
