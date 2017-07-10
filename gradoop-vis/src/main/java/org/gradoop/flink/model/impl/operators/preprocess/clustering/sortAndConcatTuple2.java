package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 *
 */

public class sortAndConcatTuple2 implements MapFunction<Tuple2<String, String>, Tuple1<String>> {
    @Override
    public Tuple1<String> map(Tuple2<String, String> in) throws Exception {
        if (in.f0.compareTo(in.f1) < 0 )
            return Tuple1.of(in.f0+","+in.f1);
        return Tuple1.of(in.f1+","+in.f0);
    }
}
