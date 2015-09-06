package com.maxdemarzi.processing;

import com.maxdemarzi.processing.centrality.DegreeArrayStorageParallelSPI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static com.maxdemarzi.processing.TestUtils.*;

public class DegreeCentralityTest {
    private GraphDatabaseService db;
    private static Service service;
    public static final int CPUS = Runtime.getRuntime().availableProcessors();
    static ExecutorService pool = Utils.createPool(CPUS, CPUS * 25);

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
    public void shouldCalculateInDegreeCentrality() throws IOException {
        String response = service.inDegree("Movie", "ACTED_IN", db);
        assertEquals("InDegree Centrality for Movie and ACTED_IN Completed!", response);
    }

    @Test
    public void shouldCalculateOutDegreeCentrality() throws IOException {
        String response = service.outDegree("Person", "ACTED_IN", db);
        assertEquals("OutDegree Centrality for Person and ACTED_IN Completed!", response);
    }

    @Test
    public void shouldCalculateDegreeCentrality() throws IOException {
        String response = service.degree("Person", "ACTED_IN", db);
        assertEquals("Degree Centrality for Person and ACTED_IN Completed!", response);
    }

    @Test
    public void shouldCalculateInDegreeCentralityArrayStorageSPI() throws IOException {
        DegreeArrayStorageParallelSPI inDegree = new DegreeArrayStorageParallelSPI(db, pool, Direction.INCOMING);
        inDegree.compute("Movie", "ACTED_IN", 1);
        long id = (long) getMovieEntry("The Matrix", db).get("id");
        assertTrue("InDegree Centrality calculted incorrectly", 5 == inDegree.getResult(id));
    }

    @Test
    public void shouldCalculateOutDegreeCentralityArrayStorageSPI() throws IOException {
        DegreeArrayStorageParallelSPI outDegree = new DegreeArrayStorageParallelSPI(db, pool, Direction.OUTGOING);
        outDegree.compute("Person", "ACTED_IN", 1);
        long id = (long) getPersonEntry("Tom Hanks", db).get("id");
        assertTrue("outDegree Centrality calculted incorrectly", 12 == outDegree.getResult(id));
    }

    @Test
    public void shouldCalculateDegreeCentralityArrayStorageSPI() throws IOException {
        DegreeArrayStorageParallelSPI degree = new DegreeArrayStorageParallelSPI(db, pool, Direction.BOTH);
        degree.compute("Person", "ACTED_IN", 1);
        long id = (long) TestUtils.getPersonEntry("Tom Hanks", db).get("id");
        assertTrue("outDegree Centrality calculted incorrectly", 12 == degree.getResult(id));
    }




}
