package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Created by alieh on 6/20/17.
 */
public class filterCorrectLinks implements FlatMapFunction <Tuple3<String, String, Boolean>,Tuple2<String, String>> {
    private Boolean filter;
    public filterCorrectLinks (Boolean FilterFlag){
        filter = FilterFlag;
    }
    @Override
    public void flatMap(Tuple3<String, String, Boolean> in, Collector<Tuple2<String, String>> out) throws Exception {
        if (in.f2.equals(filter))
            out.collect(Tuple2.of(in.f0, in.f1));
    }
}
