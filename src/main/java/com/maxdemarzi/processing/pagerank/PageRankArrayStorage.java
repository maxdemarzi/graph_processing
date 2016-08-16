package com.maxdemarzi.processing.pagerank;

import com.maxdemarzi.processing.NodeCounter;
import org.neo4j.graphdb.*;

import java.util.Arrays;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankArrayStorage implements PageRank {
    private final GraphDatabaseService db;
    private final int nodes;
    private float[] dst;

    public PageRankArrayStorage(GraphDatabaseService db) {
        this.db = db;
        this.nodes = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {

        float[] src = new float[nodes];
        dst = new float[nodes];

        RelationshipType relationshipType = DynamicRelationshipType.withName(type);

        try ( Transaction tx = db.beginTx()) {
            int[] degrees = computeDegrees(label, relationshipType);

            for (int iteration = 0; iteration < iterations; iteration++) {

                startIteration(src, dst, degrees);

                for( Relationship relationship : db.getAllRelationships()) {
                    if (relationship.isType(relationshipType)) {
                        int x = (int) relationship.getStartNode().getId();
                        int y = (int) relationship.getEndNode().getId();
                        dst[y] += src[x];
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
        return dst != null ? dst[((int) node)] : 0;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }

}