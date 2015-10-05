package com.maxdemarzi.processing.centrality;

import com.maxdemarzi.processing.NodeCounter;
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;

import java.util.HashSet;
import java.util.Set;

import static com.maxdemarzi.processing.Utils.getSingleSourceShortestPath;

public class Betweenness implements Centrality {
    private final GraphDatabaseAPI db;
    private final int nodeCount;
    private BetweennessCentrality<Double> betweennessCentrality;

    public Betweenness(GraphDatabaseService db){
        this.db = (GraphDatabaseAPI) db;
        this.nodeCount = new NodeCounter().getNodeCount(db);
    }

    @Override
    public void compute(String label, String type, int iterations) {
        SingleSourceShortestPath<Double> singleSourceShortestPath = getSingleSourceShortestPath(DynamicRelationshipType.withName(type));
        Set<Node> nodes = new HashSet<>();
        try ( Transaction tx = db.beginTx()) {
            ResourceIterator<Node> iterator = db.findNodes(DynamicLabel.label(label));
            while (iterator.hasNext()) {
                nodes.add(iterator.next());
            }
            betweennessCentrality = new BetweennessCentrality<Double>(
                    singleSourceShortestPath, nodes);
            betweennessCentrality.calculate();
        }

    }

    @Override
    public double getResult(long node) {
            if (betweennessCentrality.isCalculated()) {
                Double result = betweennessCentrality.getCentrality(db.getNodeById(node));
                if (result != null) {
                    return result;
                }
            }
        return 0.0;
    }

    @Override
    public long numberOfNodes() {
        return nodeCount;
    }

    @Override
    public String getPropertyName() {
        return  "betweeness_centrality";
    }
}
