package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by alieh on 6/15/17.
 *
 */
public class Vertex2VertexGrdpId implements MapFunction<Vertex, Tuple2<Vertex, String>>
{
    @Override
    public Tuple2<Vertex, String> map(Vertex in) throws Exception {
        return Tuple2.of(in, in.getId().toString());///
    }
}
