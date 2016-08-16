package com.maxdemarzi.processing;

import com.maxdemarzi.processing.unionfind.UnionFind;
import com.maxdemarzi.processing.unionfind.UnionFindMapStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class UnionFindTest {
    public static final double EXPECTED = 2.0;
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
    public void shouldUnionFind() throws IOException {
        String response = service.unionFind("Person", "KNOWS", 0, db);
        assertEquals("UnionFind for Person and KNOWS Completed!", response);
    }

    @Test
    public void shouldGetUnionFindMapStorage() throws IOException {
        UnionFind unionFind = new UnionFindMapStorage(db);
        unionFind.compute("Person", "KNOWS", 20);
        long id = (long) getEntry("Tom Hanks").get("id");
        assertEquals(EXPECTED, unionFind.getResult(id),0.1D);
        //dump(unionFind);
    }

    private void dump(UnionFind unionFind) {
        try (Transaction tx = db.beginTx()){
            for (int nodeId = 0; nodeId < unionFind.numberOfNodes(); nodeId++) {
                Node node = db.getNodeById(nodeId);
                System.out.printf("%s = %d -> %.5f saved: %.5f %n", node.getProperty("name", "movie"), nodeId, unionFind.getResult(nodeId), (double)node.getProperty("unionfind",0D));
            }
        }
    }

    private Map<String, Object> getEntry(String name) {
        try (Result result = db.execute(TestObjects.PERSON_RESULT_QUERY, Collections.singletonMap("name", name))) {
            return result.next();
        }
    }
}
