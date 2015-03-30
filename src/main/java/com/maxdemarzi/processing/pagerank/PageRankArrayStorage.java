package com.maxdemarzi.processing.pagerank;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.Arrays;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankArrayStorage implements PageRank {
    private final GraphDatabaseService db;
    private final int nodes;
    private float[] dstMap;

    public PageRankArrayStorage(GraphDatabaseService db) {
        this.db = db;
        NeoStoreProvider neoStoreProvider = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(NeoStoreProvider.class);
        this.nodes = (int) neoStoreProvider.evaluate().getNodeStore().getHighId();
    }

    @Override
    public void compute(String label, String type, int iterations) {

        float[] srcMap = new float[nodes];
        dstMap = new float[nodes];

        RelationshipType relationshipType = DynamicRelationshipType.withName(type);

        try ( Transaction tx = db.beginTx()) {
            int[] degreeMap = computeDegrees(label, relationshipType);

            for (int iteration = 0; iteration < iterations; iteration++) {

                startIteration(srcMap, dstMap, degreeMap);

                for( Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                    if (relationship.isType(relationshipType)) {
                        int x = (int) relationship.getStartNode().getId();
                        int y = (int) relationship.getEndNode().getId();
                        dstMap[y] += srcMap[x];
                    }
                }
            }
            tx.success();
        }
    }

    private void startIteration(float[] srcMap, float[] dstMap, int[] degreeMap) {
        for (int node = 0; node < this.nodes; node++) {
            if (degreeMap[node] == -1) continue;
            srcMap[node]= (float) (ALPHA * dstMap[node] / degreeMap[node]);
        }
        Arrays.fill(dstMap, (float) ONE_MINUS_ALPHA);
    }

    private int[] computeDegrees(String label, RelationshipType relationshipType) {
        int[] degreeMap = new int[nodes];
        Arrays.fill(degreeMap,-1);
        ResourceIterator<Node> nodes = db.findNodes(DynamicLabel.label(label));
        while (nodes.hasNext()) {
            Node node = nodes.next();
            degreeMap[((int) node.getId())]=node.getDegree(relationshipType, Direction.OUTGOING);
        }
        return degreeMap;
    }

    @Override
    public double getResult(long node) {
        return dstMap != null ? dstMap[((int) node)] : 0;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }
}
