package com.maxdemarzi.processing;

import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

@Path("/v1")
public class Service {
    private Long2DoubleMap srcMap;
    private Long2DoubleMap dstMap;
    private Long2LongMap degreeMap;

    private static final double ALPHA = 0.85;
    private static final double ONE_MINUS_ALPHA = 1 - ALPHA;

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
        srcMap = new Long2DoubleOpenHashMap();
        dstMap = new Long2DoubleOpenHashMap();
        degreeMap = new Long2LongOpenHashMap();

        RelationshipType relationshipType = DynamicRelationshipType.withName(type);

        try ( Transaction tx = db.beginTx()) {
            ResourceIterator<Node> nodes = db.findNodes(DynamicLabel.label(label));
            while (nodes.hasNext()) {
                Node node = nodes.next();
                srcMap.put(node.getId(), 0);
                dstMap.put(node.getId(), 0);
                degreeMap.put(node.getId(), node.getDegree(relationshipType, Direction.OUTGOING));
            }

            for (int iteration = 0; iteration < 20; iteration++) {
                nodes = db.findNodes(DynamicLabel.label(label));
                while (nodes.hasNext()) {
                    Node node = nodes.next();
                    srcMap.put(node.getId(), ALPHA * dstMap.get(node.getId()) / degreeMap.get(node.getId()));
                    dstMap.put(node.getId(), ONE_MINUS_ALPHA);
                }

                for( Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                    if (relationship.isType(relationshipType)) {
                        long x = relationship.getStartNode().getId();
                        long y = relationship.getEndNode().getId();
                        dstMap.put(y, (dstMap.get(y) + srcMap.get(x)));
                    }
                }
            }

            nodes = db.findNodes(DynamicLabel.label(label));
            while (nodes.hasNext()) {
                Node node = nodes.next();
                node.setProperty("pagerank", dstMap.get(node.getId()));
            }
            tx.success();
        }

        return "PageRank for " + label + " and " + type + " Completed!";
    }

}
