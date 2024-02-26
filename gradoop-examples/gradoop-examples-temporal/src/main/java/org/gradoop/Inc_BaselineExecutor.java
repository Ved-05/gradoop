package org.gradoop;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.algorithms.gradoop.AlgorithmExecutor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.Collections;

public class Inc_BaselineExecutor extends BaselineExecutor {
    static StringBuilder timings = new StringBuilder();
    static long startTime = Calendar.getInstance().getTimeInMillis();
    private static TemporalGraph updateTemporalGraph(TemporalGraph baseGraph, int ts,
                                                     TemporalGradoopConfig config) throws Exception {
        if (baseGraph == null) {
            return new TemporalCSVDataSource("/home/hadoop/gradoopOutput/redditGradoop/redditInputs/time=" + ts,
                    config).getTemporalGraph();
        }
        timings.append(System.currentTimeMillis() - startTime).append("\t");
        return updateGraph(baseGraph, ts, config);
    }

    public static void main(String[] args) throws Exception {

        verifyInputArgumentsOrExit(args, "Inc_BaselineExecutor");

        final String graph = args[0];
        final String algorithm = args[1];
        final String srcVertexId = toHex(args[2]);
        final int start = Integer.parseInt(args[3]);
        final int end = Integer.parseInt(args[4]);
        final int increment = Integer.parseInt(args[5]);
        final String inputDirectory = graph.equals("LDBC") ? "/data/hadoop/wicmi/baseline-inputs/gradoop/" + graph + "/time=" : "/home/hadoop/gradoopOutput/redditGradoop/redditInputs/time=";
        final String outputDirectory = "/home/hadoop/jan-baseline/results/iGradoop/" + graph + "/" + algorithm;

        final File resultsFile = new File(outputDirectory + "/compute_time.csv");
        if (!resultsFile.getParentFile().exists()) {
            if (!resultsFile.getParentFile().mkdirs())
                throw new Exception("Could not create parent directories for output resultsFile");
        }

        System.out.println("Graph ->" + graph);
        System.out.println("Algorithm ->" + algorithm);
        System.out.println("Source Vertex Id ->" + srcVertexId);
        System.out.println("Execution Range -> [" + start + ", " + end + "]");
        System.out.println("Input Dir. ->" + inputDirectory);
        System.out.println("Output Dir. ->" + outputDirectory);

        final FileWriter fr = new FileWriter(resultsFile, true);
        final BufferedWriter br = new BufferedWriter(fr);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final TemporalGradoopConfig config = TemporalGradoopConfig.createConfig(env);
        TemporalGraph inputGraph = null;
        startTime = System.currentTimeMillis();
        for (int i = start; i <= end; i += increment) {
            inputGraph = updateTemporalGraph(inputGraph, i, config);
            inputGraph.callForGraph(new AlgorithmExecutor<>(algorithm, getSourceVertexId(inputGraph, srcVertexId), i))
                    .writeTo(new TemporalCSVDataSink(outputDirectory + "/results/" + i, config), false);

            br.write(i + "," + env.execute().getNetRuntime() + timings + "\n");
            br.flush();
            if (i % increment == 0) System.out.println("Finished " + i + " iterations.");
            timings = new StringBuilder();
            startTime = System.currentTimeMillis();
        }
        br.close();
        fr.close();
    }

    private static GradoopId getSourceVertexId(TemporalGraph inputGraph, String srcVertexId) throws Exception {
        return inputGraph.getVertices()
                .filter(ver -> ver.getId().toString().equals(srcVertexId))
                .collect()
                .get(0)
                .getId();
    }

    private static String toHex(String num) {
        long a = Long.parseLong(num);
        String hex = Long.toString(a, 16);
        int len = hex.length();
        int left = 24 - len;
        return repeat(Math.max(0, left)) + hex;
    }

    private static String repeat(int times) {
        return String.join("", Collections.nCopies(times, "0"));
    }

    public static TemporalGraph updateGraph(TemporalGraph oldGraph, int ts, TemporalGradoopConfig config) throws Exception {
        startTime = System.currentTimeMillis();
        String deltasDirectory = "/home/hadoop/gradoopOutput/redditGradoop/redditDeltas/time=" + ts;
        TemporalDataSource dataSource = new TemporalCSVDataSource(deltasDirectory, config);
        TemporalGraph graphAtI = dataSource.getTemporalGraph();
        timings.append(System.currentTimeMillis() - startTime).append("\t");
        startTime = System.currentTimeMillis();
        graphAtI = oldGraph.combine(graphAtI);
        timings.append(System.currentTimeMillis() - startTime).append("\t");
        startTime = System.currentTimeMillis();
        graphAtI = deleteGraphMut(graphAtI, ts, config);
        timings.append(System.currentTimeMillis() - startTime).append("\t");
        return graphAtI;
    }

    public static TemporalGraph deleteGraphMut(TemporalGraph newGraph, int ts, TemporalGradoopConfig config) throws Exception {
        String tempDiffPath = "/home/hadoop/gradoopOutput/redditGradoop/redditDeletes/time=" + ts;
        TemporalDataSource dataSource = new TemporalCSVDataSource(tempDiffPath, config);
        TemporalGraph delGraph = dataSource.getTemporalGraph();

        TemporalGraph overlapgraph = newGraph.overlap(delGraph);
        DataSet<TemporalEdge> deletededges = overlapgraph.getEdges().map(ver -> {
            ver.setProperty("stoptime", PropertyValue.create((double) ts));
            return ver;
        });

        DataSet<TemporalVertex> deletedvertices = overlapgraph.getVertices().map(ver -> {
            ver.setProperty("stoptime", PropertyValue.create((double) ts));
            return ver;
        });
        TemporalGraph retainedgraph = newGraph.exclude(delGraph);

        return newGraph.getFactory()
                .fromDataSets(newGraph.getGraphHead(), retainedgraph.getVertices().union(deletedvertices), retainedgraph.getEdges().union(deletededges)).verify();
    }
}
