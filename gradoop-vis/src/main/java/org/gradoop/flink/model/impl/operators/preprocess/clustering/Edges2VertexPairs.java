package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */
public class Edges2VertexPairs {
    private DataSet<Vertex> vertices;
    private DataSet<Edge> edges;
    public Edges2VertexPairs (DataSet<Vertex> Vertices, DataSet<Edge> Edges){
        vertices = Vertices;
        edges = Edges;
    }
    public DataSet<Tuple3<Vertex, Vertex, Double>> getPairs () throws Exception {
        DataSet<Tuple2<Vertex, String>> vertex_gradoopId = vertices.map(new Vertex2VertexGrdpId());
        DataSet<Tuple3<String, String, Double>> srcId_trgtId = edges.map(new Edge2SrcIdTrgtId());
//        DataSet<Tuple3<Vertex, String, Double>> with = vertex_gradoopId.join(srcId_trgtId).where(1).equalTo(0).with(new Join1());

        return     vertex_gradoopId.join(srcId_trgtId).where(1).equalTo(0).with(new Join1())
        .join(vertex_gradoopId).where(1).equalTo(1).with(new Join2());
    }
}
