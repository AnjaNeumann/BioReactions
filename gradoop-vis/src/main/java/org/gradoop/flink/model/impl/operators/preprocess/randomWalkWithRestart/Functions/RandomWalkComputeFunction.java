package org.gradoop.flink.model.impl.operators.preprocess.randomWalkWithRestart.Functions;

import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.LongValue;
import org.gradoop.flink.model.impl.operators.preprocess.randomWalkWithRestart.RandomWalk;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;

/**
 * Created by rostami on 30.05.17.
 * Random Walk Compute Function
 */
public class RandomWalkComputeFunction extends ComputeFunction<Long, Double, Double, Double> {
    private static final long serialVersionUID = 1L;
    private final int numOfVertices;
    private Configuration conf;
    private DoubleSumAggregator aggregator;
    HashMap<Long, Long> mapIdOutDegree = new HashMap<>();

    public RandomWalkComputeFunction(Configuration conf, Graph<Long,Double,Double> g) throws Exception {
        this.conf= conf;
        this.numOfVertices = (int) g.numberOfVertices();
        for(Tuple2<Long,LongValue> t : g.outDegrees().collect()) {
            mapIdOutDegree.put(t.f0,t.f1.getValue());
        }
    }

    @Override
    public void compute(Vertex<Long, Double> vertex, MessageIterator<Double> messageIterator) throws Exception {
        BigDecimal stateProbability;
        BigDecimal difference;
        //giraph starts with 0 gelly with 1
        if (this.getSuperstepNumber() > 1) {
            BigDecimal previousStateProbability = BigDecimal.valueOf(vertex.getValue());

            stateProbability = this.calcNewStateProbability(vertex, messageIterator, teleportationProbability());

            stateProbability = stateProbability.divide(this.getPreviousCumulativeProbability(), MathContext.DECIMAL128);
            difference = stateProbability.subtract(previousStateProbability);

            this.aggregator = this.getIterationAggregator(RandomWalk.L1_NORM_OF_PROBABILITY_DIFFERENCE);
            this.aggregator.aggregate(difference.doubleValue());
        } else {
            stateProbability = this.initialProbability();
        }

        this.setNewVertexValue(stateProbability.doubleValue());
        this.aggregator = this.getIterationAggregator(RandomWalk.CUMULATIVE_PROBABILITY);
        System.out.println("vertex " + vertex);
        System.out.println("aggregator " + aggregator);
        this.aggregator.aggregate(vertex.getValue());

        if (mapIdOutDegree.get(vertex.getId()) == 0) {
            this.aggregator = this.getIterationAggregator(RandomWalk.NUM_DANGLING_VERTICES);
            this.aggregator.aggregate(1.0d);

            this.aggregator = this.getIterationAggregator(RandomWalk.CUMULATIVE_DANGLING_PROBABILITY);
            this.aggregator.aggregate(vertex.getValue());
        }


        BigDecimal sP = stateProbability;
        if (this.getSuperstepNumber() < conf.getInteger(RandomWalk.MAX_ITERATIONS, 0)) {
            for (Edge<Long, Double> edge : this.getEdges()) {
                BigDecimal eV = BigDecimal.valueOf(edge.getValue());
                BigDecimal transitionProbability = sP.multiply(eV);
                this.sendMessageTo(edge.getTarget(), transitionProbability.doubleValue());
            }
        }
    }

    /**
     * Calculates the new state probability for a vertex based upon the previous superstep
     *
     * @param vertex                   Vertex whose state probability is to be calculated
     * @param messageIterator          All incoming messages from the last superstep for this vertex
     * @param teleportationProbability Teleportaion probability
     * @return Updated state probability
     */
    protected BigDecimal calcNewStateProbability(Vertex<Long, Double> vertex, MessageIterator<Double> messageIterator, BigDecimal teleportationProbability) throws Exception {
        int numSourceVertices = conf.getInteger(RandomWalk.NUM_SOURCE_VERTICES, 0);
        if (numSourceVertices <= 0) {
            System.err.println("ERROR: No source vertex found");
        }
        BigDecimal stateProbability = BigDecimal.ZERO;
        while (messageIterator.hasNext()) {
            stateProbability = stateProbability.add(BigDecimal.valueOf(messageIterator.next()));
        }

        stateProbability = stateProbability.add(this.getDanglingProbability().divide(
                BigDecimal.valueOf(numOfVertices)));

        stateProbability = stateProbability.multiply(BigDecimal.ONE.subtract(teleportationProbability));

        for (int i = 0; i < conf.getInteger(RandomWalk.NUM_SOURCE_VERTICES, 0); i++) {
            if (vertex.getId() == conf.getLong(RandomWalk.SOURCE_VERTEX + "_" + i, 0L)) {
                stateProbability = stateProbability.add(teleportationProbability.divide(BigDecimal.valueOf(numSourceVertices)));
            }
        }
        return stateProbability;
    }

    /**
     * Returns the initial probability which is the same for all vertices
     *
     * @return Initial probability, i.e. 1/number-of-vertices
     */
    protected BigDecimal initialProbability() throws Exception {
        return BigDecimal.ONE.divide(BigDecimal.valueOf(numOfVertices));
    }

    /**
     * Due to rounding errors the probabilities do not sum up to exactly 1.
     * Therefore the cumulativeProbability is needed to minimize these errors.
     *
     * @return Cumulative probability of the previous superstep
     */
    protected BigDecimal getPreviousCumulativeProbability() {
        DoubleValue prevComProb = this.getPreviousIterationAggregate(RandomWalk.CUMULATIVE_PROBABILITY);
        return BigDecimal.valueOf(prevComProb.getValue());
    }

    /**
     * Returns the dangling proabability, i.e. the probability to be in a vertex that has no outgoing edges
     *
     * @return Returns the probability to be in a dangling vertex in the last superstep
     */
    protected BigDecimal getDanglingProbability() {
        return BigDecimal.valueOf(((DoubleSumAggregator) this.getIterationAggregator(RandomWalk.CUMULATIVE_DANGLING_PROBABILITY)).getAggregate().getValue());
    }

    /**
     * Returns the (constant) teleportation probability
     *
     * @return Teleportation Probability
     */
    protected BigDecimal teleportationProbability() {
        return BigDecimal.valueOf(conf.getDouble(RandomWalk.TELEPORTATION_PROBABILITY, 0.0f));
    }
}
