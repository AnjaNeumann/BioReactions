package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by alieh on 6/20/17.
 */

public class sortAndConcatTuple4 implements MapFunction<Tuple4<String, String, Double, String>, Tuple3<String, Double, String>> {
    @Override
    public Tuple3<String, Double, String> map(Tuple4<String, String, Double, String> in) throws Exception {
        if (in.f0.compareTo(in.f1) < 0 )
            return Tuple3.of(in.f0+","+in.f1, in.f2, in.f3);
        return Tuple3.of(in.f1+","+in.f0, in.f2, in.f3);
    }
}
