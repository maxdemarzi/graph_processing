package com.maxdemarzi.processing.pagerank;

import com.maxdemarzi.processing.Algorithm;

/**
 * @author mh
 * @since 28.03.15
 */
public interface PageRank extends Algorithm {
    double ALPHA = 0.85;
    double ONE_MINUS_ALPHA = 1 - ALPHA;

    void compute(String label, String type, int iterations);
    double getResult(long node);
    long numberOfNodes();
    default String getPropertyName() {
        return "pagerank";
    }
}
