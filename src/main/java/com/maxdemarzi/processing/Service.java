package com.maxdemarzi.processing;

import it.unimi.dsi.fastutil.longs.*;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

@Path("/v1")
public class Service {

    public static final int WRITE_BATCH = 10_000;
    public static final int ITERATIONS = 5;

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
                           @Context GraphDatabaseService db) {

        PageRank pageRank = new PageRankArrayStorageSPI(db);
        pageRank.computePageRank(label,type, ITERATIONS);
        writeBackResults(db,pageRank);

        return "PageRank for " + label + " and " + type + " Completed!";
    }

    static ExecutorService pool = createPool(4,100);

    private static ExecutorService createPool(int threads, int queueSize) {
        return new ThreadPoolExecutor(1, threads, 30, TimeUnit.SECONDS, new LinkedBlockingDeque<>(queueSize),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private void writeBackResults(GraphDatabaseService db, PageRank pageRank) {
        final long nodes = pageRank.numberOfNodes();
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
                            double value = pageRank.getRankOfNode(node);
                            if (value > 0) {
                                db.getNodeById(node).setProperty("pagerank", value);
                            }
                        }
                        tx.success();
                    }
                }
            });
            futures.add(future);
        }
        waitForTasks(futures);
    }

    private int waitForTasks(List<Future> futures) {
        int total = 0;
        for (Future future : futures) {
            try {
                future.get();
                total ++;
            } catch (InterruptedException e) {
                // ignore
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        futures.clear();
        return total;
    }

}
