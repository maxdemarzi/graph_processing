package com.maxdemarzi.processing;

import com.maxdemarzi.processing.labelpropagation.LabelPropagation;
import com.maxdemarzi.processing.labelpropagation.LabelPropagationMapStorage;
import com.maxdemarzi.processing.pagerank.*;
import com.maxdemarzi.processing.unionfind.UnionFind;
import com.maxdemarzi.processing.unionfind.UnionFindMapStorage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@Path("/v1")
public class Service {

    public static final int WRITE_BATCH = 10_000;
    public static final int CPUS = Runtime.getRuntime().availableProcessors();
    static ExecutorService pool = Utils.createPool(CPUS, CPUS*25);

    @GET
    @Path("/helloworld")
    public String helloWorld() {
        return "Hello World!";
    }

    @GET
    @Path("/warmup")
    public String warmUp(@Context GraphDatabaseService db) {
        try ( Transaction tx = db.beginTx()) {
            for ( Node n : GlobalGraphOperations.at(db).getAllNodes()) {
                n.getPropertyKeys();
                for ( Relationship relationship : n.getRelationships()) {
                    relationship.getPropertyKeys();
                    relationship.getStartNode();
                }
            }
        }
        return "Warmed up and ready to go!";
    }

    @GET
    @Path("/pagerank/{label}/{type}")
    public String pageRank(@PathParam("label") String label,
                           @PathParam("type") String type,
                           @DefaultValue("20") @QueryParam("iterations") int iterations,
                           @Context GraphDatabaseService db) {

        PageRankArrayStorageParallelSPI pageRank = new PageRankArrayStorageParallelSPI(db,pool);
        pageRank.compute(label, type, iterations);
        pageRank.writeBackResults();

        return "PageRank for " + label + " and " + type + " Completed!";
    }

    @GET
    @Path("/labelpropagation/{label}/{type}")
    public String labelPropagation(@PathParam("label") String label,
                           @PathParam("type") String type,
                           @DefaultValue("20") @QueryParam("iterations") int iterations,
                           @Context GraphDatabaseService db) {

        LabelPropagation labelPropagation = new LabelPropagationMapStorage(db);
        labelPropagation.compute(label, type, iterations);
        writeBackResults(db, labelPropagation);

        return "LabelPropagation for " + label + " and " + type + " Completed!";
    }

    @GET
    @Path("/unionfind/{label}/{type}")
    public String unionFind(@PathParam("label") String label,
                                   @PathParam("type") String type,
                                   @DefaultValue("0") @QueryParam("iterations") int iterations,
                                   @Context GraphDatabaseService db) {

        UnionFind unionFind = new UnionFindMapStorage(db);
        unionFind.compute(label, type, iterations);
        writeBackResults(db, unionFind);

        return "UnionFind for " + label + " and " + type + " Completed!";
    }

    private void writeBackResults(GraphDatabaseService db, Algorithm algorithm) {
        String propertyName = algorithm.getPropertyName();
        final long nodes = algorithm.numberOfNodes();
        int batches = (int) nodes / WRITE_BATCH;
        List<Future> futures = new ArrayList<>(batches);
        for (int node = 0; node < nodes; node += WRITE_BATCH) {
            final int start = node;
            Future future = pool.submit(new Runnable() {
                public void run() {
                    try (Transaction tx = db.beginTx()) {
                        for (long i = 0; i < WRITE_BATCH; i++) {
                            long node = i + start;
                            if (node >= nodes) break;
                            double value = algorithm.getResult(node);
                            if (value > 0) {
                                db.getNodeById(node).setProperty(propertyName, value);
                            }
                        }
                        tx.success();
                    }
                }
            });
            futures.add(future);
        }
        Utils.waitForTasks(futures);
        

    }
}
