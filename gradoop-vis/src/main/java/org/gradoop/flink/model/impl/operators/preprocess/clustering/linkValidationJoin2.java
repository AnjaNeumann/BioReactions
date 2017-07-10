package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by alieh on 6/21/17.
 */

public class linkValidationJoin2 implements JoinFunction<Tuple3<String, Double, String>, Tuple1<String>, Tuple4<String, Double, String,  Boolean>> {
    @Override
    public Tuple4<String, Double, String,  Boolean> join(Tuple3<String, Double, String> in1, Tuple1<String> in2) throws Exception {
        if (in2 == null)
            return Tuple4.of(in1.f0, in1.f1, in1.f2, false);
        return Tuple4.of(in1.f0, in1.f1, in1.f2, true);
    }
}
