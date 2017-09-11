package org.neo4j.traversals;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TerminateOnProductTraversal {

    @Context
    public GraphDatabaseService graphDatabaseService;

    @Procedure(name = "traversals.maxWeightedProductPaths", mode = Mode.READ)
    public Stream<WeightAndPathResult> traverseMaxWeightedProductPaths(
            @Name("startNode") Node startNode, @Name("maxDepth") long maxDepthLong, @Name("threshold") double threshold) {

        int maxDepth = (int)maxDepthLong;
        Map<Node, WeightAndPathResult> results = new HashMap<>();

        InitialBranchState.State<Double> ibs = new InitialBranchState.State<>(1.0, 1.0);

        final TraversalDescription traversalDescription = graphDatabaseService.traversalDescription()
                .breadthFirst()
                .expand(PathExpanders.forDirection(Direction.OUTGOING), ibs)
                .evaluator(new PathEvaluator<Double>() {
                    @Override
                    public Evaluation evaluate(Path path, BranchState<Double> state) {

                        if (path.length()==0) {
                            return Evaluation.EXCLUDE_AND_CONTINUE;
                        }

                        double weight = state.getState() * getDoubleProperty(path.lastRelationship(), "weight");

                        if (weight < threshold) {
                            return Evaluation.EXCLUDE_AND_PRUNE;
                        } else {
                            state.setState(weight);

                            Node lastNode = path.endNode();
                            WeightAndPathResult weightAndPathResult = results.get(lastNode);
                            if ((weightAndPathResult == null) || (weightAndPathResult.max < weight)) {
                                results.put(lastNode, new WeightAndPathResult(weight, path));
                            }
                            return path.length() < maxDepth ? Evaluation.EXCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_PRUNE;
                        }
                    }

                    private double getDoubleProperty(Relationship relationship, String propertyKey) {
                        Object prop = relationship.getProperty(propertyKey);
                        double lastWeight;

                        if (prop instanceof Double) {
                            lastWeight = (double)prop;
                        } else if (prop instanceof Long) {
                            lastWeight = ((Long)prop).doubleValue();
                        } else {
                            throw new IllegalArgumentException("invalid property type " + prop + " on " + relationship);
                        }
                        return lastWeight;
                    }

                    @Override
                    public Evaluation evaluate(Path path) {
                        throw new UnsupportedOperationException();
                    }
                });

        for (Path p : traversalDescription.traverse(startNode)) {
            // intentionally empty to exhaust iterator
        }

        return results.values().stream();
    }

    public class WeightAndPathResult {
        public double max;
        public Path path;
        public WeightAndPathResult(double max, Path path) {
            this.max = max;
            this.path = path;
        }
    }
}
