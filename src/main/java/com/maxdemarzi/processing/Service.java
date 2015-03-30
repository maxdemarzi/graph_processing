package com.maxdemarzi.processing;

import com.maxdemarzi.processing.labelpropagation.LabelPropagation;
import com.maxdemarzi.processing.labelpropagation.LabelPropagationMapStorage;
import com.maxdemarzi.processing.pagerank.PageRank;
import com.maxdemarzi.processing.pagerank.PageRankArrayStorage;
import com.maxdemarzi.processing.pagerank.PageRankArrayStorageSPI;
import com.maxdemarzi.processing.pagerank.PageRankMapStorage;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
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

        PageRank pageRank = new PageRankMapStorage(db);
        pageRank.compute(label, type, 20);
        writeBackResults(db,pageRank);

        return "PageRank for " + label + " and " + type + " Completed!";
    }

    @GET
    @Path("/pagerank2/{label}/{type}")
    public String pageRank2(@PathParam("label") String label,
                            @PathParam("type") String type,
                            @Context GraphDatabaseService db) {

        PageRank pageRank = new PageRankArrayStorage(db);
        pageRank.compute(label, type, 20);
        writeBackResults(db,pageRank);

        return "PageRank for " + label + " and " + type + " Completed!";
    }

    @GET
    @Path("/pagerank3/{label}/{type}")
    public String pageRank3(@PathParam("label") String label,
                            @PathParam("type") String type,
                            @Context GraphDatabaseService db) {

        PageRank pageRank = new PageRankArrayStorageSPI(db);
        pageRank.compute(label, type, 20);
        writeBackResults(db,pageRank);

        return "PageRank for " + label + " and " + type + " Completed!";
    }

    @GET
    @Path("/labelpropagation/{label}/{type}")
    public String labelPropagation(@PathParam("label") String label,
                           @PathParam("type") String type,
                           @Context GraphDatabaseService db) {

        LabelPropagation labelPropagation = new LabelPropagationMapStorage(db);
        labelPropagation.compute(label, type, 20);
        writeBackResults(db, labelPropagation);

        return "LabelPropagation for " + label + " and " + type + " Completed!";
    }

    private void writeBackResults(GraphDatabaseService db, Algorithm algorithm) {
        Transaction tx = db.beginTx();
        int counter = 0;
        String propertyName = algorithm.getPropertyName();
        try {
            for (long node = 0; node < algorithm.numberOfNodes(); node ++) {
                double value = algorithm.getResult(node);
                if (value > 0) {
	              db.getNodeById(node).setProperty(propertyName, value);
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
