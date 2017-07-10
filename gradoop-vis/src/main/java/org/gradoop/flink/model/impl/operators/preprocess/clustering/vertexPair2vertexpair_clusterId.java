package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 */
public class vertexPair2vertexpair_clusterId implements FlatMapFunction <Tuple3<Vertex, Vertex, Double>, Tuple4<String, String, Double, String>> {
    @Override
    public void flatMap(Tuple3<Vertex, Vertex, Double> in, Collector<Tuple4<String, String, Double, String>> out) throws Exception {
        String ClusterId0 = in.f0.getPropertyValue("ClusterId").toString();
        String ClusterId1 = in.f1.getPropertyValue("ClusterId").toString();
        String[] ClusterId0Array = new String[ClusterId0.split(",").length];
        String[] ClusterId1Array = new String[ClusterId1.split(",").length];
        if (ClusterId0.contains(","))
            ClusterId0Array = ClusterId0.split(",");
        else ClusterId0Array[0] = ClusterId0;
        if (ClusterId1.contains(","))
            ClusterId1Array = ClusterId1.split(",");
        else ClusterId1Array[0] = ClusterId1;
        ArrayList<String> ClusterIdsList = new ArrayList<>(Arrays.asList(ClusterId0Array));
        for (int i = 0; i< ClusterId1Array.length; i++){
            if (!ClusterIdsList.contains(ClusterId1Array[i]))
                ClusterIdsList.add(ClusterId1Array[i]);
        }
        for (String id: ClusterIdsList)
            out.collect(Tuple4.of(in.f0.getPropertyValue("recId").toString(), in.f1.getPropertyValue("recId").toString(), in.f2, id));
    }
}











































