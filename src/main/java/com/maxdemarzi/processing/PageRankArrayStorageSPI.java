package com.maxdemarzi.processing;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.tooling.GlobalGraphOperations;

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
        NeoStoreProvider neoStoreProvider = this.db.getDependencyResolver().resolveDependency(NeoStoreProvider.class);
        this.nodes = (int) neoStoreProvider.evaluate().getNodeStore().getHighId();

    }

    @Override
    public void computePageRank(String label, String type, int iterations) {

        float[] srcMap = new float[nodes];
        dst = new float[nodes];

        try ( Transaction tx = db.beginTx()) {

            ThreadToStatementContextBridge ctx = this.db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.instance().readOperations();
            int labelId = ops.labelGetForName(label);
            int typeId = ops.relationshipTypeGetForName(type);

            int[] degreeMap = computeDegrees(ops,labelId, typeId);

            RelationshipVisitor<RuntimeException> visitor = (relId, relTypeId, startNode, endNode) -> {
                if (relTypeId == typeId) {
                    dst[((int) endNode)] += srcMap[(int) startNode];
                }
            };

            for (int iteration = 0; iteration < iterations; iteration++) {
                startIteration(srcMap, dst, degreeMap);

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

    private void startIteration(float[] srcMap, float[] dstMap, int[] degreeMap) {
        for (int node = 0; node < this.nodes; node++) {
            if (degreeMap[node] == -1) continue;
            srcMap[node]= (float) (ALPHA * dstMap[node] / degreeMap[node]);
        }
        Arrays.fill(dstMap, (float) ONE_MINUS_ALPHA);
    }

    private int[] computeDegrees(ReadOperations ops, int labelId, int relationshipId) throws EntityNotFoundException {
        int[] degreeMap = new int[nodes];
        Arrays.fill(degreeMap,-1);
        PrimitiveLongIterator nodes = ops.nodesGetForLabel(labelId);
        while (nodes.hasNext()) {
            long node = nodes.next();
            degreeMap[((int)node)]= ops.nodeGetDegree(node, Direction.OUTGOING, relationshipId);
        }
        return degreeMap;
    }

    @Override
    public double getRankOfNode(long node) {
        return dst != null ? dst[((int) node)] : 0;
    }

    @Override
    public long numberOfNodes() {
        return nodes;
    }
}
