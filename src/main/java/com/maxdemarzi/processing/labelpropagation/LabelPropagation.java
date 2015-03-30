package com.maxdemarzi.processing.labelpropagation;

import com.maxdemarzi.processing.Algorithm;

public interface LabelPropagation extends Algorithm {

    void compute(String label, String type, int iterations);
    double getResult(long node);
    long numberOfNodes();
    default public String getPropertyName() {
        return "label";
    }

}
