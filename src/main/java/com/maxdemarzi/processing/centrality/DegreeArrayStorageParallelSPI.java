package com.maxdemarzi.processing.centrality;

import com.maxdemarzi.processing.NodeCounter;
import com.maxdemarzi.processing.OpsRunner;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;

import static com.maxdemarzi.processing.Utils.runOperations;

public class DegreeArrayStorageParallelSPI implements Centrality {
    static final int BATCH_SIZE  = 100_000;
    private final GraphDatabaseAPI db;
    private final int nodeCount;
    private final ExecutorService pool;
    private int[] degree;
    private String directionName = "";
    private final Direction direction;

    public DegreeArrayStorageParallelSPI(GraphDatabaseService db, ExecutorService pool, Direction direction) {
        this.pool = pool;
        this.db = (GraphDatabaseAPI) db;
        this.nodeCount = new NodeCounter().getNodeCount(db);
        this.direction = direction;
        if (!direction.equals(Direction.BOTH)) {
            directionName = direction.name().toLowerCase() + "_";
        }
    }

    @Override
    public void compute(String label, String type, int iterations) {
        degree = new int[nodeCount];
        Arrays.fill(degree, -1);



        try ( Transaction tx = db.beginTx()) {
            ThreadToStatementContextBridge ctx = this.db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations ops = ctx.get().readOperations();
            int labelId = ops.labelGetForName(label);
            int relationshipTypeId = ops.relationshipTypeGetForName(type);

            PrimitiveLongIterator it = ops.nodesGetForLabel(labelId);
            int totalCount = nodeCount;
            runOperations(pool, it, totalCount, ops, new OpsRunner() {
                public void run(int id) throws EntityNotFoundException {
                    degree[id] = ops.nodeGetDegree(id, direction, relationshipTypeId);
                }
            });

            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public double getResult(long node) {
        return degree != null ? degree[(int) node] : 0;
    }

    @Override
    public long numberOfNodes() {
        return 0;
    }

    @Override
    public String getPropertyName() {
        return  directionName + "degree_centrality";
    }
}
