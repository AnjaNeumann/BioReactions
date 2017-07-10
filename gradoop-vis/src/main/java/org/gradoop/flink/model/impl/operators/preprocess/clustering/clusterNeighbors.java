package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class clusterNeighbors implements FlatMapFunction <Tuple3<Vertex, Vertex, Double>, Tuple1<String>> {
    private String ClusterId;
    public clusterNeighbors (String DesiredClusterId){
        ClusterId = DesiredClusterId;
    }
    @Override
    public void flatMap(Tuple3<Vertex, Vertex, Double> in, Collector<Tuple1<String>> out) throws Exception {
        String clusterId0 = in.f0.getPropertyValue("ClusterId").toString();
        String clusterId1 = in.f1.getPropertyValue("ClusterId").toString();
        List<String> clusterId0List = new ArrayList<>();
        List<String> clusterId1List = new ArrayList<>();
        if (clusterId0.contains(","))
            clusterId0List = Arrays.asList(clusterId0.split(","));
        if (clusterId1.contains(","))
            clusterId1List = Arrays.asList(clusterId1.split(","));


        if ((clusterId0.equals(ClusterId)|| clusterId0List.contains(ClusterId)) && (!clusterId1.equals(ClusterId) && !clusterId1List.contains(ClusterId))) {
            if (!clusterId1.contains(","))
                out.collect(Tuple1.of(clusterId1));
            else {
                for (String id:clusterId1List)
                    out.collect(Tuple1.of(id));
            }
        }
        else if ((clusterId1.equals(ClusterId)|| clusterId1List.contains(ClusterId)) && (!clusterId0.equals(ClusterId) && !clusterId0List.contains(ClusterId))) {
            if (!clusterId0.contains(","))
                out.collect(Tuple1.of(clusterId0));
            else {
                for (String id:clusterId0List)
                    out.collect(Tuple1.of(id));
            }
        }
    }
}
