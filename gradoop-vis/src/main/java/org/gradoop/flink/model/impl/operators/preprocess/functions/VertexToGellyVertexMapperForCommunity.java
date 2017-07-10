package org.gradoop.flink.model.impl.operators.preprocess.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;

public class VertexToGellyVertexMapperForCommunity
        implements MapFunction<org.gradoop.common.model.impl.pojo.Vertex, Vertex<GradoopId, Long>> {
    //LogicalGraph graph;
    public VertexToGellyVertexMapperForCommunity() {
      //  this.graph = graph;
    }

    @Override
    public Vertex<GradoopId, Long> map(org.gradoop.common.model.impl.pojo.Vertex vertex) throws Exception {
        Vertex<GradoopId, Long> v = new Vertex<>();
        v.setId(vertex.getId());
        v.setValue(1L);
        //v.set
        //v.setField('tst',);
        return v;
    }
}