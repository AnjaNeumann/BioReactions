package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */
public class clusterNeighborVertices implements JoinFunction<Tuple1<String>, Tuple2<Vertex, String>, Tuple2<Vertex, String>> {
    @Override
    public Tuple2<Vertex, String> join(Tuple1<String> in1, Tuple2<Vertex, String> in2) throws Exception {
        return in2;
    }
}
