package com.maxdemarzi.processing;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.Collections;
import java.util.Map;

public class TestUtils {
    static public Map<String, Object> getPersonEntry(String name, GraphDatabaseService db) {
        try (Result result = db.execute(TestObjects.PERSON_RESULT_QUERY, Collections.singletonMap("name", name))) {
            return result.next();
        }
    }

    static public Map<String, Object> getMovieEntry(String title, GraphDatabaseService db) {
        try (Result result = db.execute(TestObjects.MOVIE_RESULT_QUERY, Collections.singletonMap("title", title))) {
            return result.next();
        }
    }
}
