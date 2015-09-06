package com.maxdemarzi.processing;

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;

public interface OpsRunner {
    void run(int node) throws EntityNotFoundException;
}
