package com.maxdemarzi.processing.unionfind;

import com.maxdemarzi.processing.Algorithm;

public interface UnionFind extends Algorithm {

    void compute(String label, String type);
    double getResult(long node);
    long numberOfNodes();
    default String getPropertyName() {
        return "unionfind";
    }

}
