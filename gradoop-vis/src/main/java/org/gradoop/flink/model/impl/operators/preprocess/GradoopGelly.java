package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.labelpropagation.functions.EdgeToGellyEdgeMapper;
import org.gradoop.flink.algorithms.labelpropagation.functions.VertexToGellyVertexMapper;
import org.gradoop.flink.model.impl.LogicalGraph;

public class GradoopGelly {
    public static Graph<GradoopId, Double, Double> gradoopToGellyForPageRank(LogicalGraph logicalGraph) {
        // prepare vertex set for Gelly vertex centric iteration
        DataSet<Vertex<GradoopId, Double>> vertices =
                logicalGraph.getVertices().map(new MapFunction<org.gradoop.common.model.impl.pojo.Vertex, Vertex<GradoopId, Double>>() {
                    @Override
                    public Vertex<GradoopId, Double> map(org.gradoop.common.model.impl.pojo.Vertex vertex) throws Exception {
                        return new Vertex<GradoopId,Double>(vertex.getId(),0.1d);
                    }
                });

        // prepare edge set for Gelly vertex centric iteration
        DataSet<org.apache.flink.graph.Edge<GradoopId, Double>> edges =
                logicalGraph.getEdges().map(new MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, Double>>() {
                    @Override
                    public org.apache.flink.graph.Edge<GradoopId, Double> map(Edge edge) throws Exception {
                        return new org.apache.flink.graph.Edge<GradoopId,Double>(edge.getSourceId(),edge.getTargetId(),0.1d);
                    }
                });

        // create Gelly graph
        Graph<GradoopId, Double, Double> gellyGraph = Graph.fromDataSet(
                vertices, edges, logicalGraph.getConfig().getExecutionEnvironment());

        return gellyGraph;
    }
}
