package com.maxdemarzi.processing.unionfind;

import com.maxdemarzi.processing.NodeCounter;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

/*
    Weighted quick-union with path compression
    See https://www.cs.princeton.edu/~rs/AlgsDS07/01UnionFind.pdf
 */

public class UnionFindMapStorage implements UnionFind {
    private final GraphDatabaseService db;
    private final long nodes;
    private Long2IntOpenHashMap rankMap;
    private Long2LongMap rootMap;

    public UnionFindMapStorage(GraphDatabaseService db) {
        this.db = db;
        this.rootMap = new Long2LongOpenHashMap();
        this.rankMap = new Long2IntOpenHashMap();
        this.nodes = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {
        RelationshipType relationshipType = DynamicRelationshipType.withName(type);

        try ( Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodes = db.findNodes(DynamicLabel.label(label));
            while (nodes.hasNext()) {
                long nodeId = nodes.next().getId();
                rootMap.put(nodeId, nodeId);
                rankMap.put(nodeId, 1);
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

            // This part is technically not necessary since we can just follow the unionfind property
            // of any node UP the tree to see if it's really connected or not.

            int iteration = 0;
            boolean done = false;

            while (!done && iterations > 0) {
                done = true;
                iteration++;
                nodes = db.findNodes(DynamicLabel.label(label));
                while (nodes.hasNext()) {
                    long x = nodes.next().getId();
                    if (rootMap.get(x) != x) {
                        done = false;
                        // This can be changed to be the GrandParent instead of Parent for faster convergence.
                        rootMap.put(x, rootMap.get(rootMap.get(x)));
                    }
                }

                if (iteration > iterations) {
                    done = true;
                }
            }
        }
    }

    @Override
    public double getResult(long node) {
        return rootMap != null ? rootMap.getOrDefault(node, -1L) : -1;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    };
}