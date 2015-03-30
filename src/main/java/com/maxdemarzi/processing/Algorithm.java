package com.maxdemarzi.processing;

public abstract interface Algorithm {

    public abstract void compute(String label, String type, int iterations);
    public abstract double getResult(long node);
    public abstract long numberOfNodes();
    public abstract String getPropertyName();

}
