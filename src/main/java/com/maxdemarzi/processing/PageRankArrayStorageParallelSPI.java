package com.maxdemarzi.processing;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankArrayStorageParallelSPI implements PageRank {
    private final GraphDatabaseAPI db;
    private final int nodeCount;
    private final ExecutorService pool;
    private float[] dst;

    public PageRankArrayStorageParallelSPI(GraphDatabaseService db, ExecutorService pool) {
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.nodeCount = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void computePageRank(String label, String type, int iterations) {

        float[] src = new float[nodeCount];
        dst = new float[nodeCount];

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
        for (int node = 0; node < this.nodeCount; node++) {
            if (degrees[node] == -1) continue;
            src[node]= (float) (ALPHA * dst[node] / degrees[node]);
        }
        Arrays.fill(dst, (float) ONE_MINUS_ALPHA);
    }

    private int[] computeDegrees(ReadOperations ops, int labelId, int relationshipId) throws EntityNotFoundException {
        int[] degree = new int[nodeCount];
        Arrays.fill(degree,-1);
        PrimitiveLongIterator it = ops.nodesGetForLabel(labelId);
        int totalCount = nodeCount;
        runOperations(it, totalCount, ops, new OpsRunner() {
            public void run(int id) throws EntityNotFoundException {
                degree[id] = ops.nodeGetDegree(id, Direction.OUTGOING, relationshipId);
            }
        });
        return degree;
    }

    private void runOperations(final PrimitiveLongIterator it, int totalCount, ReadOperations ops, OpsRunner runner) {
        List<Future> futures = new ArrayList<>((int)(totalCount / BATCH_SIZE));
        while (it.hasNext()) {
            futures.add(pool.submit(new BatchRunnable(ops, it, BATCH_SIZE,runner)));
        }
        Utils.waitForTasks(futures);
    }

    static final int BATCH_SIZE  = 100_000;

    static class BatchRunnable implements Runnable, OpsRunner {
        final long[] ids;
        final ReadOperations ops;
        private final OpsRunner runner;
        int offset =0;

        public BatchRunnable(ReadOperations ops, PrimitiveLongIterator iterator, int batchSize, OpsRunner runner) {
            ids = add(iterator,batchSize);
            this.ops = ops;
            this.runner = runner;
        }

        private long[] add(PrimitiveLongIterator it, int count) {
            long[] ids = new long[count];
            while (count--> 0 && it.hasNext()) {
                ids[offset++]=it.next();
            }
            return ids;
        }

        public void run() {
            int notFound = 0;
            for (int i=0;i<offset;i++) {
                try {
                    run((int) ids[i]);
                } catch (EntityNotFoundException e) {
                    notFound++;
                }
            }
            if (notFound > 0 ) System.err.println("Entities not found "+notFound);
        }

        @Override
        public void run(int node) throws EntityNotFoundException {
            runner.run(node);
        }
    }

    interface OpsRunner {
        void run(int node) throws EntityNotFoundException;
    }
    @Override
    public double getRankOfNode(long node) {
        return dst != null ? dst[((int) node)] : 0;
    }

    @Override
    public long numberOfNodes() {
        return nodeCount;
    }
}
