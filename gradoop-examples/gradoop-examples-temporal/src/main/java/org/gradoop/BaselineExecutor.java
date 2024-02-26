package org.gradoop;

public class BaselineExecutor {
    protected static void verifyInputArgumentsOrExit(String[] args, String className) {
        if (args.length == 0) {
            String classPath = "java org.gradoop." + className;
            System.out.println("Usage: java " + classPath + "<full-jar-path> <graph> <algorithm> <src vertex id> <start time step> <end time step>");
            System.out.println("Example: ./flink/bin/flink run -c " + classPath +
                    "/home/hadoop/jan-baseline/jars/gradoop.jar Reddit EAT 4912335 1 122");
            System.out.println("Example: ./flink/bin/flink run -c " + classPath +
                    "/home/hadoop/jan-baseline/jars/gradoop.jar LDBC SSSP 3165345 1 365");
            System.exit(1);
        }
    }
}
