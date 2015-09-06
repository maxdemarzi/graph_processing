package com.maxdemarzi.processing.centrality;

import com.maxdemarzi.processing.Algorithm;

public interface Centrality extends Algorithm {
    void compute(String label, String type, int iterations);
    double getResult(long node);
    long numberOfNodes();
    default public String getPropertyName() {
        return "centrality";
    }
}
