package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by alieh on 6/21/17.
 */
public class clusterAndneighborsJoin2 implements JoinFunction<Tuple4<String, String, Double, String>, Tuple1<String>, Tuple4<String, String, Double, String>> {

    @Override
    public Tuple4<String, String, Double, String> join(Tuple4<String, String, Double, String> in1, Tuple1<String> in2) throws Exception {
        return in1;
    }
}
