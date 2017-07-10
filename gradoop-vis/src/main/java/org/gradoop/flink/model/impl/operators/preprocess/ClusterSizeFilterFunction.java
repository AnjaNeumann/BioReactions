package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by rostam on 14.06.17.
 * ClusterSizeFilterFunction
 */
public class ClusterSizeFilterFunction implements FilterFunction<Tuple2<String, Integer>> {
    int min, max;

    public ClusterSizeFilterFunction(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public boolean filter(Tuple2<String, Integer> t) throws Exception {
        return t.f1 >= min && t.f1 <= max;
    }
}
