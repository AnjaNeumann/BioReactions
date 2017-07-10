package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by alieh on 6/15/17.
 */
public class Join1 implements JoinFunction<Tuple2<Vertex, String>, Tuple3<String, String, Double>, Tuple3<Vertex, String, Double>> {
    @Override
    public Tuple3<Vertex, String, Double> join(Tuple2<Vertex, String> in1, Tuple3<String, String, Double> in2) throws Exception {
        return Tuple3.of(in1.f0, in2.f1, in2.f2);
    }
}
