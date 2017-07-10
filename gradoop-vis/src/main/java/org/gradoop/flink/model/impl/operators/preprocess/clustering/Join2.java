package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */
public class Join2 implements JoinFunction<Tuple3<Vertex, String, Double>, Tuple2<Vertex, String>, Tuple3<Vertex, Vertex, Double>> {
    @Override
    public Tuple3<Vertex, Vertex, Double> join(Tuple3<Vertex, String, Double> in1, Tuple2<Vertex, String> in2) throws Exception {
        return Tuple3.of(in1.f0, in2.f0, in1.f2);
    }
}
