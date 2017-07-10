package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.link_analysis.PageRank;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.preprocess.functions.VertexToGellyVertexMapperForPageRank;
import org.gradoop.flink.model.impl.operators.preprocess.functions.EdgeToGellyEdgeMapperForPageRank;

public class PageRankSampling implements UnaryGraphToGraphOperator {
    private final double threshold;
    private double dampeningFactor;

    public PageRankSampling(double dampeningFactor, double threshold) {
        this.dampeningFactor = dampeningFactor;
        this.threshold = threshold;
    }

    public LogicalGraph sample(LogicalGraph graph) throws Exception {

        // prepare vertex set for Gelly vertex centric iteration
        DataSet<Vertex<GradoopId, Double>> vertices =
                graph.getVertices().map(new VertexToGellyVertexMapperForPageRank());

        // prepare edge set for Gelly vertex centric iteration
        DataSet<org.apache.flink.graph.Edge<GradoopId, Double>> edges =
                graph.getEdges().map(new EdgeToGellyEdgeMapperForPageRank());

        // create Gelly graph
        Graph<GradoopId, Double, Double> gellyGraph = Graph.fromDataSet(
                vertices, edges, graph.getConfig().getExecutionEnvironment());
        DataSet<Vertex<GradoopId, Double>>  newVertices = gellyGraph.getDegrees().map(new GellyGraphDegreeMapFunction());

        // create new Gelly graph
        Graph<GradoopId, Double, Double> newGellyGraph = Graph.fromDataSet(
                newVertices, edges, graph.getConfig().getExecutionEnvironment());

        DataSet<PageRank.Result<GradoopId>> res= newGellyGraph.run(new PageRank<>(dampeningFactor,100));
//        res.map(new MapFunction<PageRank.Result<GradoopId>, Object>() {
//            @Override
//            public Object map(PageRank.Result<GradoopId> gradoopIdResult) throws Exception {
//                return gradoopIdResult.;
//            }
//        })
        DataSet<PageRank.Result<GradoopId>> res2 =res.filter(new FilterFunction<PageRank.Result<GradoopId>>() {
            @Override
            public boolean filter(PageRank.Result<GradoopId> gradoopIdResult) throws Exception {
                return gradoopIdResult.getPageRankScore().getValue() < threshold;
            }
        });
        //DataSet<Vertex<GradoopId,Double>> pageRankResult =  newGellyGraph.run(new PageRank<>(dampeningFactor, 100));
        //DataSet<Vertex<GradoopId,Double>> filtered = pageRankResult.filter(new PageRankResultFilterFunction(threshold));

//        graph.getVertices().filter(new FilterFunction<org.gradoop.common.model.impl.pojo.Vertex>() {
//            @Override
//            public boolean filter(org.gradoop.common.model.impl.pojo.Vertex vertex) throws Exception {
//                return vertex.getProperties().get());
//            }
//        })

        DataSet<org.gradoop.common.model.impl.pojo.Vertex> new_vertices = res2.map(new MapFunction<PageRank.Result<GradoopId>, org.gradoop.common.model.impl.pojo.Vertex>() {
            @Override
            public org.gradoop.common.model.impl.pojo.Vertex map(PageRank.Result<GradoopId> gradoopIdResult) throws Exception {
                org.gradoop.common.model.impl.pojo.Vertex v = new org.gradoop.common.model.impl.pojo.Vertex();
                v.setId(gradoopIdResult.getVertexId0());
                v.setProperty("PageRankScore",gradoopIdResult.getPageRankScore());
                return v;
            }
        });
        // return labeled graph
        LogicalGraph lg =  LogicalGraph.fromDataSets(
                new_vertices, graph.getEdges(), graph.getConfig());

        DataSet<org.gradoop.common.model.impl.pojo.Vertex> fromOriginalVertices =
                graph.getVertices()
                .join(lg.getVertices())
                .where(new Id<>())
                .equalTo(new Id<>())
                .with(new LeftSide<>());

        DataSet<Edge> newEdges = graph.getEdges()
                .join(lg.getVertices())
                .where(new SourceId<>())
                .equalTo(new Id<org.gradoop.common.model.impl.pojo.Vertex>())
                .with(new LeftSide<Edge, org.gradoop.common.model.impl.pojo.Vertex>())
                .join(lg.getVertices())
                .where(new TargetId<>())
                .equalTo(new Id<org.gradoop.common.model.impl.pojo.Vertex>())
                .with(new LeftSide<Edge, org.gradoop.common.model.impl.pojo.Vertex>());

        return LogicalGraph.fromDataSets(
               fromOriginalVertices , newEdges, graph.getConfig());
    }

    @Override
    public LogicalGraph execute(LogicalGraph graph) {
        try {
            return sample(graph);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return graph;
    }

    @Override
    public String getName() {
        return PageRankSampling.class.getName();
    }
}
