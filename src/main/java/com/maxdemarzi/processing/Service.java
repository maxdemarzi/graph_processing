package com.maxdemarzi.processing;

import it.unimi.dsi.fastutil.longs.*;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

@Path("/v1")
public class Service {

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
        pageRank.computePageRank(label,type,20);
        writeBackResults(db,pageRank);

        return "PageRank for " + label + " and " + type + " Completed!";
    }

    private void writeBackResults(GraphDatabaseService db, PageRank pageRank) {
        Transaction tx = db.beginTx();
        int counter = 0;
        try {
            for (long node = 0;node < pageRank.numberOfNodes(); node ++) {
                double value = pageRank.getRankOfNode(node);
                if (value > 0) {
	              db.getNodeById(node).setProperty("pagerank", value);
                  if (++counter % 10_000 == 0) {
      	            tx.success();
      	            tx.close();
      	            tx = db.beginTx();
        	      }
                }
            }
        } finally {
            tx.success();
            tx.close();
        }
    }

}
