package com.maxdemarzi.processing.pagerank;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankMapStorage implements PageRank {
    private final GraphDatabaseService db;
    private final int nodes;
    private Long2DoubleMap dstMap;

    public PageRankMapStorage(GraphDatabaseService db) {
        this.db = db;
        NeoStoreProvider neoStoreProvider = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(NeoStoreProvider.class);
        this.nodes = (int) neoStoreProvider.evaluate().getNodeStore().getHighId();
    }

    @Override
    public void compute(String label, String type, int iterations) {
        Long2DoubleMap srcMap = new Long2DoubleOpenHashMap();
        Long2LongMap degreeMap = new Long2LongOpenHashMap();
        dstMap = new Long2DoubleOpenHashMap(nodes);

        RelationshipType relationshipType = DynamicRelationshipType.withName(type);

        try ( Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodes = db.findNodes(DynamicLabel.label(label));
            while (nodes.hasNext()) {
                Node node = nodes.next();
                srcMap.put(node.getId(), 0);
                dstMap.put(node.getId(), 0);
                degreeMap.put(node.getId(), node.getDegree(relationshipType, Direction.OUTGOING));
            }

            for (int iteration = 0; iteration < iterations; iteration++) {
                nodes = db.findNodes(DynamicLabel.label(label));
                while (nodes.hasNext()) {
                    Node node = nodes.next();
                    srcMap.put(node.getId(), ALPHA * dstMap.get(node.getId()) / degreeMap.get(node.getId()));
                    dstMap.put(node.getId(), ONE_MINUS_ALPHA);
                }

                for( Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                    if (relationship.isType(relationshipType)) {
                        long x = relationship.getStartNode().getId();
                        long y = relationship.getEndNode().getId();
                        dstMap.put(y, (dstMap.get(y) + srcMap.get(x)));
                    }
                }
            }
            tx.success();
        }
    }

    @Override
    public double getResult(long node) {
        return dstMap != null ? dstMap.get(node) : -1;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }
}
