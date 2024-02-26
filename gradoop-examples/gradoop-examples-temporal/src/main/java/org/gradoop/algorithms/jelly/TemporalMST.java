package org.gradoop.algorithms.jelly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;

public class TemporalMST implements GraphAlgorithm<GradoopId, Tuple2<Long, Long>, Tuple3<Integer, Long, Long>,
        DataSet<Vertex<GradoopId, Tuple2<GradoopId, Long>>>> {

    private final GradoopId srcVertexId;

    private final int maxIterations;

    /**
     * Creates an instance of the SingleSourceShortestTemporalPathEAT algorithm
     *
     * @param srcVertexId The ID of the source vertex.
     */
    public TemporalMST(GradoopId srcVertexId, int maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<GradoopId, Tuple2<GradoopId, Long>>> run(Graph<GradoopId, Tuple2<Long, Long>, Tuple3<Integer, Long, Long>> input) {
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.setName("Temporal MST");
        parameters.setSolutionSetUnmanagedMemory(true);

        return input.mapVertices(new InitVerticesMapper(srcVertexId))
                .runScatterGatherIteration(new MinDistanceMessengerForTupleWithSrcVertex(),
                        new VertexDistanceUpdaterWithSrcVertex(this.maxIterations), maxIterations, parameters)
                .mapVertices(new FinalVerticesMapper())
                .getVertices();
    }

    private static final class InitVerticesMapper implements MapFunction<Vertex<GradoopId, Tuple2<Long, Long>>,
            Tuple3<Long, Long, Tuple2<GradoopId, Long>>> {
        private final GradoopId srcVertexId;

        public InitVerticesMapper(GradoopId srcId) {
            this.srcVertexId = srcId;
        }

        public Tuple3<Long, Long, Tuple2<GradoopId, Long>> map(Vertex<GradoopId, Tuple2<Long, Long>> v) {
            return new Tuple3<>(v.f1.f0, v.f1.f1,
                    v.f0.equals(this.srcVertexId) ? new Tuple2<>(srcVertexId, 0L) : new Tuple2<>(null, Long.MAX_VALUE));
        }
    }

    private static final class MinDistanceMessengerForTupleWithSrcVertex extends ScatterFunction<GradoopId, Tuple3<Long, Long, Tuple2<GradoopId, Long>>,
            Tuple2<GradoopId, Long>, Tuple3<Integer, Long, Long>> {
        @Override
        public void sendMessages(Vertex<GradoopId, Tuple3<Long, Long, Tuple2<GradoopId, Long>>> vertex) {
            Tuple2<GradoopId, Long> cArrivedAt = vertex.f1.f2;
            if (cArrivedAt.f1 < Integer.MAX_VALUE) {
                for (Edge<GradoopId, Tuple3<Integer, Long, Long>> edge : getEdges()) {
                    long eStartTime = edge.f2.f1;
                    long targetArrivalTime = Math.max(cArrivedAt.f1, eStartTime) + edge.f2.f0;
                    if (cArrivedAt.f1 < eStartTime || cArrivedAt.f1 < edge.f2.f2)
                        this.sendMessageTo(edge.getTarget(), new Tuple2<>(vertex.f0, targetArrivalTime));
                }
            }
        }
    }

    private static final class VertexDistanceUpdaterWithSrcVertex extends GatherFunction<GradoopId, Tuple3<Long, Long, Tuple2<GradoopId, Long>>, Tuple2<GradoopId, Long>> {

        private final int timeStep;

        public VertexDistanceUpdaterWithSrcVertex(int timeStep) {
            this.timeStep = timeStep;
        }

        @Override
        public void updateVertex(Vertex<GradoopId, Tuple3<Long, Long, Tuple2<GradoopId, Long>>> vertex, MessageIterator<Tuple2<GradoopId, Long>> inMessages) {
            Tuple3<Long, Long, Tuple2<GradoopId, Long>> vertexProperties = vertex.f1;
            Tuple2<GradoopId, Long> currentPair = vertexProperties.f2;

            for (Tuple2<GradoopId, Long> msg : inMessages) {
                long msgTime = msg.f1;
                long currentTime = currentPair.f1;

                if (msgTime < currentTime && msgTime <= this.timeStep &&
                        vertexProperties.f0 <= msgTime && msgTime < vertexProperties.f1)
                    currentPair = msg;
                else if (msgTime == currentTime) {
                    if (msg.f0.compareTo(currentPair.f0) < 0 && msgTime <= this.timeStep &&
                            vertexProperties.f0 <= msgTime && msgTime < vertexProperties.f1)
                        currentPair = msg;
                }
            }

            if (currentPair != vertexProperties.f2) {
                vertexProperties.f2 = currentPair;
                this.setNewVertexValue(vertexProperties);
            }
        }
    }

    public static final class FinalVerticesMapper implements MapFunction<Vertex<GradoopId, Tuple3<Long, Long, Tuple2<GradoopId, Long>>>, Tuple2<GradoopId, Long>> {
        @Override
        public Tuple2<GradoopId, Long> map(Vertex<GradoopId, Tuple3<Long, Long, Tuple2<GradoopId, Long>>> v) {
            return v.f1.f2;
        }
    }
}
