package com.maxdemarzi.processing;

import com.maxdemarzi.processing.centrality.Closeness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ClosenessCentralityTest {
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
    public void shouldCalculateClosenessCentrality() throws IOException {
        String response = service.closeness("Movie", "ACTED_IN", db);
        assertEquals("Closeness Centrality for Movie and ACTED_IN Completed!", response);
    }

    @Test
    public void shouldCalculateClosenessCentralityTwo() throws IOException {
        Closeness centrality = new Closeness(db);
        centrality.compute("Person", "KNOWS", 0);

        long id = (long) TestUtils.getPersonEntry("Tom Hanks", db).get("id");
        try ( Transaction tx = db.beginTx()) {
            assertEquals(0.005235602094240838, centrality.getResult(id), 0.1D);
        }
    }
}
