package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

public class PageRankResultFilterFunction implements FilterFunction<Vertex<GradoopId, Double>> {
    private double threshold;
    public PageRankResultFilterFunction(double threshold) {
        this.threshold = threshold;
    }

    @Override
    public boolean filter(Vertex<GradoopId, Double> gradoopIdDoubleVertex) throws Exception {
        return gradoopIdDoubleVertex.f1 >= threshold;
    }
}
