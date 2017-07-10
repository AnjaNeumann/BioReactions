package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by alieh on 6/20/17.
 */
public class uniqRecIds implements GroupReduceFunction<Tuple1<String>,Tuple1<String> > {
    @Override
    public void reduce(Iterable<Tuple1<String>> in, Collector<Tuple1<String>> out) throws Exception {
        List<Tuple1<String>> ids = new ArrayList<>();
        for (Tuple1<String> i:in){
            if (!ids.contains(i))
                ids.add(i);
        }
        for (Tuple1<String> i:ids){
            out.collect(i);
        }
    }
}
