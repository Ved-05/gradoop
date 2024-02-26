package org.gradoop.algorithms.gradoop;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.algorithms.common.GradoopEdgeToGellyEdge;
import org.gradoop.algorithms.common.GradoopVertexToJellyVertex;
import org.gradoop.algorithms.jelly.*;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.GradoopGellyAlgorithm;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;

public class AlgorithmExecutor<
        G extends GraphHead,
        V extends org.gradoop.common.model.api.entities.Vertex,
        E extends Edge,
        TG extends BaseGraph<G, V, E, TG, GC>,
        GC extends BaseGraphCollection<G, V, E, TG, GC>
        > extends GradoopGellyAlgorithm<G, V, E, TG, GC, Tuple2<Long, Long>,
        Tuple3<Integer, Long, Long>> {
    private final String algorithmToExecute;
    private final int maxIterations;
    private final GradoopId srcVertexId;

    public AlgorithmExecutor(String algorithmToExecute, GradoopId srcVertexId, int maxIterations) {
        super(new GradoopVertexToJellyVertex<>(), new GradoopEdgeToGellyEdge<>());
        this.algorithmToExecute = algorithmToExecute;
        this.maxIterations = maxIterations;
        this.srcVertexId = srcVertexId;
    }

    public TG executeInGelly(Graph<GradoopId, Tuple2<Long, Long>, Tuple3<Integer, Long, Long>> gellyGraph) throws Exception {
        DataSet<V> updatedVertices = executeAlgorithm(gellyGraph);
        return this.currentGraph.getFactory().fromDataSets(this.currentGraph.getGraphHead(), updatedVertices, this.currentGraph.getEdges());
    }

    private DataSet<V> executeAlgorithm(Graph<GradoopId, Tuple2<Long, Long>, Tuple3<Integer, Long, Long>> gellyGraph) throws Exception {
        switch (this.algorithmToExecute) {
            case "EAT":
                return (new EarliestArrivalTime<>(srcVertexId, this.maxIterations))
                        .run(gellyGraph)
                        .join(this.currentGraph.getVertices())
                        .where(new int[]{0})
                        .equalTo(new Id<>())
                        .with(new AttributeJoin<>());
            case "Reachability":
                return (new Reachability<>(srcVertexId, this.maxIterations))
                        .run(gellyGraph)
                        .join(this.currentGraph.getVertices())
                        .where(new int[]{0})
                        .equalTo(new Id<>())
                        .with(new AttributeJoin<>());
            case "SSSP":
                return (new SingleSourceShortestPath<>(srcVertexId, this.maxIterations))
                        .run(gellyGraph)
                        .join(this.currentGraph.getVertices())
                        .where(new int[]{0})
                        .equalTo(new Id<>())
                        .with(new AttributeJoin<>());
            case "TMST":
                return (new TemporalMST(srcVertexId, this.maxIterations))
                        .run(gellyGraph)
                        .join(this.currentGraph.getVertices())
                        .where(new int[]{0})
                        .equalTo(new Id<>())
                        .with(new AttributeJoin<>());
            case "FAST":
                return (new FastestPathDuration<>(srcVertexId, this.maxIterations))
                        .run(gellyGraph)
                        .join(this.currentGraph.getVertices())
                        .where(new int[]{0})
                        .equalTo(new Id<>())
                        .with(new AttributeJoin<>());
            default:
                throw new IllegalArgumentException("Algorithm not supported");
        }
    }


    private static final class AttributeJoin<V extends org.gradoop.common.model.api.entities.Vertex, AnsType>
            implements JoinFunction<Vertex<GradoopId, AnsType>, V, V> {

        private final String resultAttribute;

        public AttributeJoin() {
            this.resultAttribute = "result";
        }

        @Override
        public V join(org.apache.flink.graph.Vertex<GradoopId, AnsType> gellyVertex, V vertex) {
            vertex.setProperty(resultAttribute, gellyVertex.getValue());
            return vertex;
        }
    }
}
