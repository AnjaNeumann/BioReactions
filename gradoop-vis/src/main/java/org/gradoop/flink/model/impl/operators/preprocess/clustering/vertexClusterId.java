package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Input: Vertex
 * Output: Tuple2<Vertex,ClusterId>
 * conf: if "isCombined = true", The clusterId of vertices belonging to multiple clusters is returned as Tuple2<Vertex,"X,Y">, otherwise, the output is
 * Tuple2<Vertex,"X"> and Tuple2<Vertex,"Y">
 */
public class vertexClusterId implements FlatMapFunction<Vertex, Tuple2<Vertex, String>> {
    private boolean isCombined;
    public vertexClusterId (boolean IsCombined){
        isCombined = IsCombined;
    }
    @Override
    public void flatMap (Vertex in, Collector<Tuple2<Vertex, String>> out) throws Exception {

        String clusterId = in.getPropertyValue("ClusterId").toString();
        if (!clusterId.contains(",") || (clusterId.contains(",") && isCombined))
            out.collect(Tuple2.of(in, clusterId));
        else {
            String[] clusterIds = clusterId.split(",");
            for (String id:clusterIds)
                out.collect(Tuple2.of(in, id));
        }
    }
}

