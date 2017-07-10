package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 *
 */
public class Edge2SrcIdTrgtId implements MapFunction <Edge, Tuple3<String, String, Double>> {
    @Override
    public Tuple3<String, String, Double> map(Edge in) throws Exception {
        return Tuple3.of(in.getSourceId().toString(), in.getTargetId().toString(), Double.parseDouble(in.getPropertyValue("value").toString()));
    }
}
