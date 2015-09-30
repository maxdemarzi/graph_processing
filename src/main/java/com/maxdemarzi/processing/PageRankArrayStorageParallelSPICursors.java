package com.maxdemarzi.processing;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.EntityItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * @author mh
 * @since 28.03.15
 */
public class PageRankArrayStorageParallelSPICursors implements PageRank {
    public static final int ONE_MINUS_ALPHA_INT = toInt(ONE_MINUS_ALPHA);
    private final GraphDatabaseAPI db;
    private final int nodeCount;
    private final ExecutorService pool;
    private final int relCount;
    private AtomicIntegerArray dst;

    public PageRankArrayStorageParallelSPICursors(GraphDatabaseService db, ExecutorService pool) {
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.nodeCount = new NodeCounter().getNodeCount(db);
        this.relCount = new NodeCounter().getRelationshipCount(db);
    }

    @Override
    public void computePageRank(String label, String type, int iterations) {

        int[] src = new int[nodeCount];
        dst = new AtomicIntegerArray(nodeCount);

        try ( Transaction tx = db.beginTx()) {

            ThreadToStatementContextBridge ctx = this.db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            int labelId = ops.labelGetForName(label);
            int typeId = ops.relationshipTypeGetForName(type);

            int[] degrees = computeDegrees(ops,labelId, typeId);

            RelationshipVisitor<RuntimeException> visitor = new RelationshipVisitor<RuntimeException>() {
                public void visit(long relId, int relTypeId, long startNode, long endNode) throws RuntimeException {
                    if (relTypeId == typeId) {
                        dst.addAndGet(((int) endNode),src[(int) startNode]);
                    }
                }
            };

            for (int iteration = 0; iteration < iterations; iteration++) {
                startIteration(src, dst, degrees);

                Cursor<RelationshipItem> rels = ops.relationshipCursorGetAll();
                runOperations(rels, relCount , ops, new OpsRunner() {
                    public void run(int id) throws EntityNotFoundException {
                        ops.relationshipVisit(id, visitor);
                    }
                });
            }
            tx.success();
        } catch (EntityNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void startIteration(int[] src, AtomicIntegerArray dst, int[] degrees) {
        for (int node = 0; node < this.nodeCount; node++) {
            if (degrees[node] == -1) continue;
            src[node]= toInt(ALPHA * toFloat(dst.getAndSet(node, ONE_MINUS_ALPHA_INT)) / degrees[node]);

        }
    }

    private static int toInt(double value) {
        return (int) (100_000*value);
    }
    private static double toFloat(int value) {
        return value / 100_000.0;
    }

    private int[] computeDegrees(ReadOperations ops, int labelId, int relationshipId) throws EntityNotFoundException {
        int[] degree = new int[nodeCount];
        Arrays.fill(degree,-1);
        Cursor<NodeItem> nodeCursor = ops.nodeCursorGetForLabel(labelId);
        int totalCount = nodeCount;
        runOperations(nodeCursor, totalCount, ops, new OpsRunner() {
            public void run(int id) throws EntityNotFoundException {
                degree[id] = ops.nodeGetDegree(id, Direction.OUTGOING, relationshipId);
            }
        });
        return degree;
    }

    private void runOperations(final Cursor it, int totalCount, ReadOperations ops, OpsRunner runner) {
        List<Future> futures = new ArrayList<>((int)(totalCount / BATCH_SIZE));
        while (it.next()) {
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

        public BatchRunnable(ReadOperations ops, Cursor cursor, int batchSize, OpsRunner runner) {
            ids = add(cursor,batchSize);
            this.ops = ops;
            this.runner = runner;
        }

        private long[] add(Cursor<EntityItem> cursor, int count) {
            long[] ids = new long[count];
            while (count--> 0 && cursor.next()) {
                ids[offset++]=cursor.get().id();
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
        public void run(int element) throws EntityNotFoundException {
            runner.run(element);
        }
    }

    interface OpsRunner {
        void run(int element) throws EntityNotFoundException;
    }

    @Override
    public double getRankOfNode(long node) {
        return dst != null ? toFloat(dst.get((int) node)) : 0;
    }

    @Override
    public long numberOfNodes() {
        return nodeCount;
    }

    public void writeBackResults() {
        ThreadToStatementContextBridge ctx = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
        int pagerankId;
        try (Transaction tx = db.beginTx()) {
            pagerankId = ctx.get().tokenWriteOperations().propertyKeyGetOrCreateForName("pagerank");
            tx.success();
        } catch (IllegalTokenNameException e) {
            throw new RuntimeException(e);
        }
        int batches = (int) nodeCount / BATCH_SIZE;
        List<Future> futures = new ArrayList<>(batches);
        for (int node = 0; node < nodeCount; node += BATCH_SIZE) {
            final int start = node;
            Future future = pool.submit(new Runnable() {
                public void run() {
                    try (Transaction tx = db.beginTx()) {
                        DataWriteOperations ops = ctx.get().dataWriteOperations();
                        for (long i = 0; i < BATCH_SIZE; i++) {
                            long node = i + start;
                            if (node >= nodeCount) break;
                            double value = getRankOfNode(node);
                            if (value > 0) {
                                ops.nodeSetProperty(node, DefinedProperty.doubleProperty(pagerankId, value));
                            }
                        }
                        tx.success();
                    } catch (ConstraintValidationKernelException | InvalidTransactionTypeKernelException | EntityNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            });
            futures.add(future);
        }
        Utils.waitForTasks(futures);
    }
}
