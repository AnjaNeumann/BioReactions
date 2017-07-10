package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.Summarization;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;

public class GraphSummary implements UnaryGraphToGraphOperator {
    private final double threshold;
    private double dampeningFactor;

    public GraphSummary(double dampeningFactor, double threshold) {
        this.dampeningFactor = dampeningFactor;
        this.threshold = threshold;
    }

    @Override
    public LogicalGraph execute(LogicalGraph graph) {
        // property key used for label propagation
//        final String communityKey = "ClusterId";

        // load the graph and set initial community id
 //       graph = graph.transformVertices((current, transformed) -> {
 //           current.setProperty(communityKey, current.getId());
 //           return current;
 //       });

        // apply label propagation to compute communities
        //graph = graph.callForGraph(new GellyLabelPropagation(10, communityKey));
        //graph = graph.callForGraph(new GradoopLabelPropagation(5,"ClusterId"));

        // prepare vertex set for Gelly vertex centric iteration
        DataSet<Vertex<GradoopId, String>> vertices =
                graph.getVertices().map(new MapFunction<org.gradoop.common.model.impl.pojo.Vertex, Vertex<GradoopId, String>>() {
                    @Override
                    public org.apache.flink.graph.Vertex<GradoopId, String> map(org.gradoop.common.model.impl.pojo.Vertex vertex) throws Exception {
                        org.apache.flink.graph.Vertex<GradoopId, String> v = new org.apache.flink.graph.Vertex<>();
                        v.setId(vertex.getId());
                        v.setValue(vertex.getPropertyValue("ClusterId").toString());
                        return v;
                    }
                });

        // prepare edge set for Gelly vertex centric iteration
        DataSet<org.apache.flink.graph.Edge<GradoopId, NullValue>> edges =
                graph.getEdges().map(new MapFunction<org.gradoop.common.model.impl.pojo.Edge, Edge<GradoopId, NullValue>>() {
                    @Override
                    public Edge<GradoopId, NullValue> map(org.gradoop.common.model.impl.pojo.Edge edge) throws Exception {
                        Edge<GradoopId, NullValue> e = new Edge<>();
                        e.setSource(edge.getSourceId());
                        e.setTarget(edge.getTargetId());
                        return e;
                    }
                });

        // create Gelly graph
        Graph<GradoopId, String, NullValue> gellyGraph = Graph.fromDataSet(
                vertices, edges, graph.getConfig().getExecutionEnvironment());

        try {
            Graph<GradoopId,Summarization.VertexValue<String>, Summarization.EdgeValue<NullValue>> g =
                    gellyGraph.run(new Summarization<GradoopId, String, NullValue>());

            DataSet<org.gradoop.common.model.impl.pojo.Vertex> nv = g.getVertices().map(new MapFunction<org.apache.flink.graph.Vertex<GradoopId, Summarization.VertexValue<String>>, org.gradoop.common.model.impl.pojo.Vertex>() {
                @Override
                public org.gradoop.common.model.impl.pojo.Vertex map(org.apache.flink.graph.Vertex<GradoopId, Summarization.VertexValue<String>> gradoopIdVertexValueVertex) throws Exception {
                    org.gradoop.common.model.impl.pojo.Vertex v = new org.gradoop.common.model.impl.pojo.Vertex();
                    v.setId(gradoopIdVertexValueVertex.getId());
                    v.setLabel(gradoopIdVertexValueVertex.f1.f0);
                    return v;
                }
            });
            DataSet<org.gradoop.common.model.impl.pojo.Edge> ne = g.getEdges().map(new MapFunction<Edge<GradoopId, Summarization.EdgeValue<NullValue>>, org.gradoop.common.model.impl.pojo.Edge>() {
                @Override
                public org.gradoop.common.model.impl.pojo.Edge map(Edge<GradoopId, Summarization.EdgeValue<NullValue>> gradoopIdEdgeValueEdge) throws Exception {
                    org.gradoop.common.model.impl.pojo.Edge e = new org.gradoop.common.model.impl.pojo.Edge();
                    e.setSourceId(gradoopIdEdgeValueEdge.getSource());
                    e.setTargetId(gradoopIdEdgeValueEdge.getTarget());
                    return e;
                }
            });
            ne = ne.filter(new FilterFunction<org.gradoop.common.model.impl.pojo.Edge>() {
                @Override
                public boolean filter(org.gradoop.common.model.impl.pojo.Edge edge) throws Exception {
                    return !edge.getSourceId().equals(edge.getTargetId());
                }
            });
            graph = LogicalGraph.fromDataSets(nv,ne,graph.getConfig());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return graph;
    }

    @Override
    public String getName() {
        return GraphSummary.class.getName();
    }
}
