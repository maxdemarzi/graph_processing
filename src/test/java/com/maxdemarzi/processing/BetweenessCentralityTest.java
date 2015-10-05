package com.maxdemarzi.processing;

import com.maxdemarzi.processing.centrality.Betweenness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class BetweenessCentralityTest {
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
    public void shouldCalculateBetweennessCentrality() throws IOException {
        String response = service.betweenness("Movie", "ACTED_IN", db);
        assertEquals("Betweenness Centrality for Movie and ACTED_IN Completed!", response);
    }

    @Test
    public void shouldCalculateBetweennessCentralityTwo() throws IOException {
        Betweenness centrality = new Betweenness(db);
        centrality.compute("Person", "KNOWS", 0);

            long id = (long) TestUtils.getPersonEntry("Tom Hanks", db).get("id");
        try ( Transaction tx = db.beginTx()) {
            assertEquals(1992.4944081374413, centrality.getResult(id), 0.1D);
        }
    }
}
