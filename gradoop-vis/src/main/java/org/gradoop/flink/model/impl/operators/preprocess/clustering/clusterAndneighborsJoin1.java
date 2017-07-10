package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.List;

/**
 *
 */
public class clusterAndneighborsJoin1 implements JoinFunction<Tuple4<String, String, Double, String>, Tuple2<List<Vertex>, String>, Tuple4<String, String, Double, String>> {

    @Override
    public Tuple4<String, String, Double, String> join(Tuple4<String, String, Double, String> in1, Tuple2<List<Vertex>, String> in2) throws Exception {
        return in1;
    }
}
