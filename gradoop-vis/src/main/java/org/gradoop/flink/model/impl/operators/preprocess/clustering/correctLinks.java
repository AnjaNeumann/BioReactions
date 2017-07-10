package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 *
 */
public class correctLinks implements JoinFunction <Tuple2<String, String>, Tuple1<String>, Tuple3<String, String, Boolean>> {
    @Override
    public Tuple3<String, String, Boolean> join(Tuple2<String, String> in1, Tuple1<String> in2) throws Exception {
        if (in2 == null)
            return Tuple3.of(in1.f0, in1.f1, false);
        return Tuple3.of(in1.f0, in1.f1, true);
    }
}
