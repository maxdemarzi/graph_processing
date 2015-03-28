package com.maxdemarzi.processing;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PageRankTest {
    private GraphDatabaseService db;
    private static Service service;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Label label = DynamicLabel.label("Person");
    private static final RelationshipType relationshipType = DynamicRelationshipType.withName("KNOWS");

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
    public void shouldGetRecommendation() throws IOException {
        String response = service.pageRank("Person", "KNOWS", db);
        assertEquals("PageRank for Person and KNOWS Completed!", response);

        dumpResults();

    }
    @Test
    public void shouldGetRecommendationArrayStorage() throws IOException {
        PageRank pageRank = new PageRankArrayStorage(db);
        pageRank.computePageRank("Person", "KNOWS", 5);
        dump(pageRank);
    }
    @Test
    public void shouldGetRecommendationMapStorage() throws IOException {
        PageRank pageRank = new PageRankMapStorage(db);
        pageRank.computePageRank("Person", "KNOWS", 5);
        dump(pageRank);
    }

    private void dump(PageRank pageRank) {
        for (int node = 0;node < pageRank.numberOfNodes();node++) {
            System.out.printf("%d -> %.5f %n", node, pageRank.getRankOfNode(node));
        }
    }

    private void dumpResults() {
        Map<String, Object> params = new HashMap<>();
        params.put( "name", "Tom Hanks" );

        Result result = db.execute(TestObjects.PERSON_PG_QUERY, params);
        Iterator<Object> pageranks = result.columnAs( "p.pagerank" );
        assertEquals( 4.642800717539658, pageranks.next() );
        System.out.println(result.resultAsString());
    }

}
