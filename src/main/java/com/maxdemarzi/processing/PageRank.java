package com.maxdemarzi.processing;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author mh
 * @since 28.03.15
 */
public interface PageRank {
    double ALPHA = 0.85;
    double ONE_MINUS_ALPHA = 1 - ALPHA;

    void computePageRank(String label, String type, int iterations);
    double getRankOfNode(long node);
    long numberOfNodes();
}
