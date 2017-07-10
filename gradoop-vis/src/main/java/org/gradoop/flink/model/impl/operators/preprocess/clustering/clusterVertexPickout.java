package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by alieh on 6/19/17.
 */
public class clusterVertexPickout implements FlatMapFunction <Tuple2<Vertex, String>, Tuple2<Vertex, String>>{
    private String clusterId;
    public clusterVertexPickout (String DesiredClusterId){
        clusterId = DesiredClusterId;
    }
    @Override
    public void flatMap(Tuple2<Vertex, String> in, Collector<Tuple2<Vertex, String>> out) throws Exception {
         if (in.f1.equals(clusterId))
             out.collect(in);
    }
}
