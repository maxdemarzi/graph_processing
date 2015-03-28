Graph Processing
================

This is an unmanaged extension with Graph Processing Algorithms on top of Neo4j.

1. Build it:

        mvn clean package

2. Copy target/graph-processing-1.0.jar to the plugins/ directory of your Neo4j server.

        mv target/graph-processing-1.0.jar neo4j/plugins/.

3. Download additional jars to the plugins/ directory of your Neo4j server.

        curl -O http://central.maven.org/maven2/it/unimi/dsi/fastutil/7.0.2/fastutil-7.0.2.jar
        mv fastutil-7.0.2.jar neo4j/plugins/.

4. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.maxdemarzi.processing=/service

5. Start Neo4j server.

6. Create the Movie Dataset:

        play :movies

7. Create KNOWS relationships amongst actors:

        MATCH (a1:Person)-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors)
        CREATE (a1)-[:KNOWS]->(coActors);

8. Call the pagerank endpoint:

        curl http://neo4j:swordfish@localhost:7474/service/v1/pagerank/Person/KNOWS

You should see "PageRank for Person and KNOWS Completed!"

9. Check the pageranks of some nodes:

        MATCH (n:Person) RETURN n ORDER BY n.pagerank DESC LIMIT 10;

