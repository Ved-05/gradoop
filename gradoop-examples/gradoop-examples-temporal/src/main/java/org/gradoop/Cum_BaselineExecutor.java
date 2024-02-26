package org.gradoop;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.algorithms.gradoop.AlgorithmExecutor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.util.TemporalGradoopConfig;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Collections;

public class Cum_BaselineExecutor extends BaselineExecutor {

    private static TemporalGraph loadTemporalGraph(String inputDirectory, TemporalGradoopConfig config) throws Exception {
        TemporalDataSource dataSource = new TemporalCSVDataSource(inputDirectory, config);
        return dataSource.getTemporalGraph();
    }

    public static void main(String[] args) throws Exception {
        verifyInputArgumentsOrExit(args, "Cum_BaselineExecutor");

        final String graph = args[0];
        final String algorithm = args[1];
        final String srcVertexId = toHex(args[2]);
        final int start = Integer.parseInt(args[3]);
        final int end = Integer.parseInt(args[4]);
        final int increment = Integer.parseInt(args[5]);
        final String inputDirectory = graph.equals("LDBC") ? "/data/hadoop/wicmi/baseline-inputs/gradoop/" + graph + "/time=" : "/home/hadoop/gradoopOutput/redditGradoop/redditInputs/time=";
        final String outputDirectory = "/home/hadoop/jan-baseline/results/gradoop/" + graph + "/" + algorithm;

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
        TemporalGraph inputGraph;
        for (int i = start; i <= end; i += increment) {
            inputGraph = loadTemporalGraph(inputDirectory + i, config);
            inputGraph.callForGraph(new AlgorithmExecutor<>(algorithm, getSourceVertexId(inputGraph, srcVertexId), i))
                    .writeTo(new TemporalCSVDataSink(outputDirectory + "/results/" + i, config), false);

            br.write(i + "," + env.execute().getNetRuntime() + "\n");
            br.flush();
            if (i % 5 == 0) System.out.println("Finished " + i + " iterations.");
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
}
