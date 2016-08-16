package com.maxdemarzi.processing.pagerank;

import com.maxdemarzi.processing.NodeCounter;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.Arrays;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankArrayStorageSPI implements PageRank {
    private final GraphDatabaseAPI db;
    private final int nodes;
    private float[] dst;

    public PageRankArrayStorageSPI(GraphDatabaseService db) {
        this.db = (GraphDatabaseAPI) db;
        this.nodes = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {

        float[] src = new float[nodes];
        dst = new float[nodes];

        try ( Transaction tx = db.beginTx()) {

            ThreadToStatementContextBridge ctx = this.db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            int labelId = ops.labelGetForName(label);
            int typeId = ops.relationshipTypeGetForName(type);

            int[] degrees = computeDegrees(ops,labelId, typeId);

            RelationshipVisitor<RuntimeException> visitor = new RelationshipVisitor<RuntimeException>() {
                public void visit(long relId, int relTypeId, long startNode, long endNode) throws RuntimeException {
                    if (relTypeId == typeId) {
                        dst[((int) endNode)] += src[(int) startNode];
                    }
                }
            };

            for (int iteration = 0; iteration < iterations; iteration++) {
                startIteration(src, dst, degrees);

                PrimitiveLongIterator rels = ops.relationshipsGetAll();
                while (rels.hasNext()) {
                    ops.relationshipVisit(rels.next(), visitor);
                }
            }
            tx.success();
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void startIteration(float[] src, float[] dst, int[] degrees) {
        for (int node = 0; node < this.nodes; node++) {
            if (degrees[node] == -1) continue;
            src[node]= (float) (ALPHA * dst[node] / degrees[node]);
        }
        Arrays.fill(dst, (float) ONE_MINUS_ALPHA);
    }

    private int[] computeDegrees(ReadOperations ops, int labelId, int relationshipId) throws EntityNotFoundException {
        int[] degrees = new int[nodes];
        Arrays.fill(degrees,-1);
        PrimitiveLongIterator nodes = ops.nodesGetForLabel(labelId);
        while (nodes.hasNext()) {
            long node = nodes.next();
            degrees[((int)node)]= ops.nodeGetDegree(node, Direction.OUTGOING, relationshipId);
        }
        return degrees;
    }

    @Override
    public double getResult(long node) {
        return dst != null ? dst[((int) node)] : -1;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }
}
