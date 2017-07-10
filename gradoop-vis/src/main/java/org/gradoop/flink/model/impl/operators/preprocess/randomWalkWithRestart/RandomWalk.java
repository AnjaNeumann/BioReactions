package org.gradoop.flink.model.impl.operators.preprocess.randomWalkWithRestart;

import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.CommunityDetection;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.gradoop.flink.model.impl.operators.preprocess.randomWalkWithRestart.Functions.RandomWalkComputeFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rostami on 30.05.17.
 *
 */

public class RandomWalk {
    public static final String MAX_ITERATIONS = RandomWalk.class.getName() + ".maxSupersteps";
    public static final String TELEPORTATION_PROBABILITY = RandomWalk.class.getName() + ".teleportationProbability";
    public static final String SOURCE_VERTEX = RandomWalk.class.getName() + ".sourceVertex";
    public static final String CUMULATIVE_PROBABILITY = RandomWalk.class.getName() + ".cumulativeProbability";
    public static final String CUMULATIVE_DANGLING_PROBABILITY = RandomWalk.class.getName() + ".cumulativeDanglingProbability";
    public static final String NUM_DANGLING_VERTICES = RandomWalk.class.getName() + ".numDanglingVertices";
    public static final String L1_NORM_OF_PROBABILITY_DIFFERENCE = RandomWalk.class.getName() + ".l1NormOfProbabilityDifference";
    public static final String NUM_SOURCE_VERTICES = RandomWalk.class.getName() + ".numSourceVertices";
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Vertex<Long, Double> v1 = new Vertex<Long, Double>(12L, (0.25d));
        Vertex<Long, Double> v2 = new Vertex<Long, Double>(34L, (0.25d));
        Vertex<Long, Double> v3 = new Vertex<Long, Double>(56L, (0.25d));
        Vertex<Long, Double> v4 = new Vertex<Long, Double>(78L, (0.25d));

        Edge<Long, Double> e1 = new Edge<Long, Double>(12L, 34L, 0.1d);
        Edge<Long, Double> e2 = new Edge<Long, Double>(12L, 56L, 0.9d);
        Edge<Long, Double> e3 = new Edge<Long, Double>(34L, 78L, 0.9d);
        Edge<Long, Double> e4 = new Edge<Long, Double>(34L, 56L, 0.1d);
        Edge<Long, Double> e5 = new Edge<Long, Double>(56L, 12L, 0.1d);
        Edge<Long, Double> e6 = new Edge<Long, Double>(56L, 34L, 0.8d);
        Edge<Long, Double> e7 = new Edge<Long, Double>(56L, 78L, 0.1d);
        Edge<Long, Double> e8 = new Edge<Long, Double>(78L, 34L, 1.0d);

        List<Vertex<Long, Double>> vertices;
        List<Edge<Long, Double>> edges;

        vertices = new ArrayList();
        vertices.add(v1);
        vertices.add(v2);
        vertices.add(v3);
        vertices.add(v4);

        edges = new ArrayList();
        edges.add(e1);
        edges.add(e2);
        edges.add(e3);
        edges.add(e4);
        edges.add(e5);
        edges.add(e6);
        edges.add(e7);
        edges.add(e8);


        Graph<Long, Double, Double> graph = Graph.fromCollection(vertices, edges, env);

        ArrayList<Long> sourceVertices = new ArrayList<Long>();
        sourceVertices.add(12L);
        int maxIterations = 50;
        double teleportationProbability = 0.15d;
        Configuration conf = new Configuration();
        conf.setInteger(RandomWalk.MAX_ITERATIONS, maxIterations);
        conf.setDouble(RandomWalk.TELEPORTATION_PROBABILITY, teleportationProbability);
        conf.setInteger(RandomWalk.NUM_SOURCE_VERTICES, sourceVertices.size());
        for (int i = 0; i < sourceVertices.size(); i++) {
            conf.setLong(RandomWalk.SOURCE_VERTEX + "_" + i, sourceVertices.get(i));
        }

        ComputeFunction<Long, Double, Double, Double> cf =  new RandomWalkComputeFunction(conf,graph);

        MessageCombiner<Long, Double> mc = new MessageCombiner<Long, Double>() {
            @Override
            public void combineMessages(MessageIterator<Double> messageIterator) throws Exception {

            }
        };


        VertexCentricConfiguration parameters = new VertexCentricConfiguration();
        parameters.setName("Gelly Iteration");
//        parameters.setOptDegrees(true);
        parameters.setOptNumVertices(true);

        parameters.registerAggregator(RandomWalk.L1_NORM_OF_PROBABILITY_DIFFERENCE, new DoubleSumAggregator());
        parameters.registerAggregator(RandomWalk.CUMULATIVE_PROBABILITY, new DoubleSumAggregator());
        parameters.registerAggregator(RandomWalk.NUM_DANGLING_VERTICES, new DoubleSumAggregator());
        parameters.registerAggregator(RandomWalk.CUMULATIVE_DANGLING_PROBABILITY, new DoubleSumAggregator());
        Graph resultGraph = graph.runVertexCentricIteration(cf,null,100,parameters);
        System.out.println("test 1 " + resultGraph.getVertices().collect());

        //double DAMPENING_FACTOR = 0.85;
        //System.out.println("test2 " + graph.run(new PageRank<>(DAMPENING_FACTOR, 100)).collect());

        Vertex<Long, Long> vv1 = new Vertex<Long, Long>(12L, 1L);
        Vertex<Long, Long> vv2 = new Vertex<Long, Long>(34L, 1L);
        Vertex<Long, Long> vv3 = new Vertex<Long, Long>(56L, 1L);
        Vertex<Long, Long> vv4 = new Vertex<Long, Long>(78L, 1L);

        Edge<Long, Double> ee1 = new Edge<Long, Double>(12L, 34L, 0.1d);
        Edge<Long, Double> ee2 = new Edge<Long, Double>(12L, 56L, 0.9d);
        Edge<Long, Double> ee3 = new Edge<Long, Double>(34L, 78L, 0.9d);
        Edge<Long, Double> ee4 = new Edge<Long, Double>(34L, 56L, 0.1d);
        Edge<Long, Double> ee5 = new Edge<Long, Double>(56L, 12L, 0.1d);
        Edge<Long, Double> ee6 = new Edge<Long, Double>(56L, 34L, 0.8d);
        Edge<Long, Double> ee7 = new Edge<Long, Double>(56L, 78L, 0.1d);
        Edge<Long, Double> ee8 = new Edge<Long, Double>(78L, 34L, 1.0d);

        List<Vertex<Long, Long>> verticess;
        List<Edge<Long, Double>> edgess;

        verticess = new ArrayList();
        verticess.add(vv1);
        verticess.add(vv2);
        verticess.add(vv3);
        verticess.add(vv4);

        edgess = new ArrayList();
        edgess.add(ee1);
        edgess.add(ee2);
        edgess.add(ee3);
        edgess.add(ee4);
        edgess.add(ee5);
        edgess.add(ee6);
        edgess.add(ee7);
        edgess.add(ee8);


        Graph<Long, Long, Double> graphs = Graph.fromCollection(verticess, edgess, env);

        CommunityDetection<Long> cd = new CommunityDetection<>(300,0.2d);
        Graph gg = graphs.run(cd);
        System.out.println("test3 " + gg.getVertices().collect());
        System.out.println("test4 " + gg.getEdges().collect());

    }
}