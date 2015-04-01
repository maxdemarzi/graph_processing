package com.maxdemarzi.processing;

import com.maxdemarzi.processing.labelpropagation.LabelPropagation;
import com.maxdemarzi.processing.labelpropagation.LabelPropagationMapStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class LabelPropagationTest {
    public static final double EXPECTED = 1.0;
    private GraphDatabaseService db;
    private static Service service;

    @Before
    public void setUp() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        service = new Service();
        populateDb(db);
    }

    private void populateDb(GraphDatabaseService db) {
        try ( Transaction tx = db.beginTx()) {
            db.execute(TestObjects.MOVIES_QUERY);
            db.execute(TestObjects.KNOWS_QUERY);
            tx.success();
        }
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    @Test
    public void shouldPropagateLabels() throws IOException {
        String response = service.labelPropagation("Person", "KNOWS", 20, db);
        assertEquals("LabelPropagation for Person and KNOWS Completed!", response);
    }

    @Test
    public void shouldGetPropagateLabelsMapStorage() throws IOException {
        LabelPropagation labelPropagation = new LabelPropagationMapStorage(db);
        labelPropagation.compute("Person", "KNOWS", 20);
        long id = (long) getEntry("Tom Hanks").get("id");
        assertEquals(EXPECTED, labelPropagation.getResult(id),0.1D);
    }

    private Map<String, Object> getEntry(String name) {
        try (Result result = db.execute(TestObjects.PERSON_PG_QUERY, Collections.singletonMap("name", name))) {
            return result.next();
        }
    }

}
