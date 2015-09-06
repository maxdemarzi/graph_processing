package com.maxdemarzi.processing;

import org.neo4j.graphdb.GraphDatabaseService;

import com.maxdemarzi.processing.pagerank.PageRank;
import com.maxdemarzi.processing.pagerank.PageRankArrayStorage;
import com.maxdemarzi.processing.pagerank.PageRankArrayStorageSPI;
import com.maxdemarzi.processing.pagerank.PageRankMapStorage;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.junit.*;

import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PageRankTest {
    public static final double EXPECTED = 4.778829041015646;
    private static GraphDatabaseService db;
    private static Service service;

    @BeforeClass
    public static void setUp() {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        service = new Service();
        populateDb(db);
    }

    private static void populateDb(GraphDatabaseService db) {
        try ( Transaction tx = db.beginTx()) {
            db.execute(TestObjects.MOVIES_QUERY);
            db.execute(TestObjects.KNOWS_QUERY);
            tx.success();
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        db.shutdown();
    }

    @Test
    public void shouldGetPageRank() throws IOException {
        String response = service.pageRank("Person", "KNOWS", 20, db);
        assertEquals("PageRank for Person and KNOWS Completed!", response);
    }

    @Test
    public void shouldGetPageRankArrayStorageSPI() throws IOException {
        PageRank pageRank = new PageRankArrayStorageSPI(db);
        pageRank.compute("Person", "KNOWS", 20);
        long id = (long) getEntry("Tom Hanks").get("id");
        assertEquals(EXPECTED, pageRank.getResult(id),0.1D);
        // dump(pageRank);
    }

    @Test
    public void shouldGetPageRankArrayStorage() throws IOException {
        PageRank pageRank = new PageRankArrayStorage(db);
        pageRank.compute("Person", "KNOWS", 20);
        long id = (long) getEntry("Tom Hanks").get("id");
        assertEquals(EXPECTED, pageRank.getResult(id),0.1D);
        //  dump(pageRank);
    }


    @Test
    public void shouldGetPageRankMapStorage() throws IOException {
        PageRank pageRank = new PageRankMapStorage(db);
        pageRank.compute("Person", "KNOWS", 20);
        long id = (long) getEntry("Tom Hanks").get("id");
        assertEquals(EXPECTED, pageRank.getResult(id),0.1D);
        // dump(pageRank);
    }

    private void dump(PageRank pageRank) {
        for (int node = 0;node < pageRank.numberOfNodes();node++) {
            System.out.printf("%d -> %.5f %n", node, pageRank.getResult(node));
        }
    }

    private void dumpResults(double expected) {
        Map<String, Object> row = getEntry("Tom Hanks");
//        assertEquals( 4.642800717539658, pageranks.next() );
        double rank = (double) row.get("pagerank");
        assertEquals(expected, rank,0.1D);
        System.out.println(row);
    }

    private Map<String, Object> getEntry(String name) {
        try (Result result = db.execute(TestObjects.PERSON_RESULT_QUERY, Collections.singletonMap("name", name))) {
            return result.next();
        }
    }

}
