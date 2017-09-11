package org.neo4j.traversals;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class TerminateOnProductTraversalTests {

    private GraphDatabaseService db;

    public static void registerProcedure(GraphDatabaseService db, Class<?>...procedures) throws KernelException {
        Procedures proceduresService = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Procedures.class);
        for (Class<?> procedure : procedures) {
            proceduresService.registerProcedure(procedure);
            proceduresService.registerFunction(procedure);
            proceduresService.registerAggregationFunction(procedure);
        }
    }


    @Before
    public void setupGraphDatabase() throws KernelException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedure(db, TerminateOnProductTraversal.class);
    }

    @After
    public void shutdownGraphDatabase() {
        db.shutdown();
    }

    @Test
    public void pathWeightsAreMultiplied() {

        db.execute("create (:Entity{id:1})-[:REL{weight:0.5}]->(:Entity{id:2})-[:REL{weight:0.8}]->(:Entity{id:3})").close();

        final List<Map<String, Object>> result = Iterators.asList(db.execute("match (s:Entity{id:1}) call traversals.maxWeightedProductPaths(s, 7, 0.3) yield max, path return max,path"));

        assertEquals(2, result.size());
        assertEquals(0.5, result.get(0).get("max"));
        assertEquals(0.5*0.8, result.get(1).get("max"));
    }

    @Test
    public void terminatedAtMaxDepth() {

        db.execute("create (:Entity{id:1})-[:REL{weight:0.9}]->(:Entity{id:2})-[:REL{weight:0.9}]->(:Entity{id:3})-[:REL{weight:0.9}]->(:Entity{id:4})").close();
        final List<Map<String, Object>> result = Iterators.asList(db.execute("match (s:Entity{id:1}) call traversals.maxWeightedProductPaths(s, 2, 0.3) yield max, path return max,path"));

        withTransaction(() -> {
            assertEquals(2, result.size());
            assertEquals(0.9, result.get(0).get("max"));
            assertEquals(2l, ((Path)(result.get(0).get("path"))).endNode().getProperty("id"));
            assertEquals(0.9*0.9, result.get(1).get("max"));
            assertEquals(3l, ((Path)(result.get(1).get("path"))).endNode().getProperty("id"));
            return null;
        });

    }

    @Test
    public void loopFreeResults() {

        db.execute("create (e1:Entity{id:1})," +
                "(e2:Entity{id:2}), " +
                "(e3:Entity{id:3}), " +
                "(e4:Entity{id:4}), " +
                "(e5:Entity{id:5}), " +
                "(e1)-[:REL{weight:0.9}]->(e2), " +
                "(e2)-[:REL{weight:0.9}]->(e3), " +
                "(e2)-[:REL{weight:0.9}]->(e4), " +
                "(e4)-[:REL{weight:0.9}]->(e5), " +
                "(e5)-[:REL{weight:0.9}]->(e3), " +
                "(e3)-[:REL{weight:0.9}]->(e2) ").close();
        final List<Map<String, Object>> result = Iterators.asList(db.execute("match (s:Entity{id:1}) call traversals.maxWeightedProductPaths(s, 10, 0.1) yield max, path return max,path"));

        withTransaction(() -> {
            assertEquals(4, result.size());

            result.stream().forEach(stringObjectMap -> {
                Path path = (Path) stringObjectMap.get("path");
                Assert.assertTrue(path.length() <= 3);
            });
            return null;
        });

    }

    private void withTransaction(Supplier<Void> supplier) {
        try (Transaction tx = db.beginTx()) {
            supplier.get();
            tx.success();
        }
    }

}
