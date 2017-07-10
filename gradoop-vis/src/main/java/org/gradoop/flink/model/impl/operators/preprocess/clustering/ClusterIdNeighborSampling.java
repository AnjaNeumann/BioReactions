/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 */
public class ClusterIdNeighborSampling implements UnaryGraphToGraphOperator {
    /**
     * Creates new RandomNodeSampling instance.
     *
     * @param clusterId The given cluster id
     */
    public String clusterId;
    DataSet<Tuple2<String, String>> goldenTruth;

    public ClusterIdNeighborSampling(String clusterId, ExecutionEnvironment env, String PMFilePath) {
        this.clusterId = clusterId;
        goldenTruth = env.readCsvFile(PMFilePath).types(String.class, String.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalGraph execute(LogicalGraph graph) {
        try {
            return get1ClusterAndNeighborsAsLG(graph,clusterId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public LogicalGraph get1ClusterAndNeighborsAsLG(LogicalGraph graph, String ClusterId) throws Exception {
        // all vertices
        DataSet<Vertex> clusteredGraphVertices = graph.getVertices();
        DataSet<Edge> inputGraphEdges = graph.getEdges();
        DataSet<Tuple2<Vertex, String>> vertex_clusterId = clusteredGraphVertices.flatMap(new vertexClusterId(false));
        DataSet<Tuple2<Vertex, String>> mainVertices_clusterId = vertex_clusterId.flatMap(new clusterVertexPickout(ClusterId));
        DataSet<Tuple3<Vertex, Vertex, Double>> vertexPairs = new Edges2VertexPairs(clusteredGraphVertices, inputGraphEdges).getPairs();
        DataSet<Tuple1<String>> neighborClusterIds = vertexPairs.flatMap(new clusterNeighbors(ClusterId));
        DataSet<Tuple2<Vertex, String>> neighborVertices_clusterId = neighborClusterIds.join(vertex_clusterId).where(0).equalTo(1).with(new clusterNeighborVertices());

        DataSet<Vertex> LogicalGraphVertices = mainVertices_clusterId.union(neighborVertices_clusterId).map(new MapFunction<Tuple2<Vertex, String>, Tuple2<Vertex, String>>() {
            @Override
            public Tuple2<Vertex, String> map(Tuple2<Vertex, String> value) throws Exception {
                return Tuple2.of(value.f0, value.f0.getId().toString());
            }
        }).distinct(1).map(new MapFunction<Tuple2<Vertex, String>, Vertex>() {
            @Override
            public Vertex map(Tuple2<Vertex, String> value) throws Exception {
                value.f0.setProperty("IsMissing", false);
                return value.f0;
            }
        });

        // all links
        DataSet<Tuple1<String>> allClusterIds = mainVertices_clusterId.union(neighborVertices_clusterId).distinct(1).map(new MapFunction<Tuple2<Vertex, String>, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(Tuple2<Vertex, String> value) throws Exception {
                return Tuple1.of(value.f1);
            }
        });
        DataSet<Tuple4<String, String, Double, String>> vertexPair_clusterId = vertexPairs.flatMap(new vertexPair2vertexpair_clusterId ());
        DataSet<Tuple4<String, String, Double, String>> allLinks  = vertexPair_clusterId.join(allClusterIds).where(3).equalTo(0).with(new clusterAndneighborsJoin2());
        DataSet<Tuple3<String, Double, String>> Tuple3AllLinks = allLinks.map(new sortAndConcatTuple4());
        DataSet<Tuple1<String>> Tuple1GoldenTruth = goldenTruth.map(new sortAndConcatTuple2());
        DataSet<Tuple5<String, String, Double, String, Boolean>> allValidatedLinks = Tuple3AllLinks.leftOuterJoin(Tuple1GoldenTruth).where(0).equalTo(0).with(new linkValidationJoin2()).distinct(0)
                .map(new MapFunction<Tuple4<String, Double, String, Boolean>, Tuple5<String, String, Double, String, Boolean>>() {
                    @Override
                    public Tuple5<String, String, Double, String, Boolean> map(Tuple4<String, Double, String, Boolean> in) throws Exception {
                        return Tuple5.of(in.f0.split(",")[0],in.f0.split(",")[1],in.f1, in.f2, in.f3);
                    }
                });
        DataSet<Tuple2<String, String>> gradoopId_recId = clusteredGraphVertices.map(new MapFunction<Vertex, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Vertex value) throws Exception {
                return Tuple2.of(value.getId().toString(), value.getPropertyValue("recId").toString());
            }
        });
        DataSet<Tuple5<String, String, Double, String, Boolean>> allValidatedLinksWithGrdpids = allValidatedLinks.join(gradoopId_recId).where(0).equalTo(1).with(new JoinFunction<Tuple5<String, String, Double, String, Boolean>, Tuple2<String, String>, Tuple5<String, String, Double, String, Boolean>>() {
            @Override
            public Tuple5<String, String, Double, String, Boolean> join(Tuple5<String, String, Double, String, Boolean> in1, Tuple2<String, String> in2) throws Exception {
                return Tuple5.of(in2.f0, in1.f1, in1.f2, in1.f3, in1.f4);
            }
        }).join(gradoopId_recId).where(1).equalTo(1).with(new JoinFunction<Tuple5<String, String, Double, String, Boolean>, Tuple2<String, String>, Tuple5<String, String, Double, String, Boolean>>() {
            @Override
            public Tuple5<String, String, Double, String, Boolean> join(Tuple5<String, String, Double, String, Boolean> in1, Tuple2<String, String> in2) throws Exception {
                return Tuple5.of(in1.f0, in2.f0, in1.f2, in1.f3, in1.f4);
            }
        });

        DataSet<Edge> LogicalGraphEdges = allValidatedLinksWithGrdpids.map(new link2Edge(graph.getConfig().getEdgeFactory()));
        //all correct links from GoldenTruth
//        DataSet<Tuple1<String>> rec_ids1 = allLinks.flatMap(new linkRecId());
        DataSet<Tuple1<String>> rec_ids2 = LogicalGraphVertices.map(new Vertex2VertexGrdpId()).map(new MapFunction<Tuple2<Vertex, String>, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(Tuple2<Vertex, String> value) throws Exception {
                return Tuple1.of(value.f1);
            }
        });
        DataSet<Tuple1<String>> rec_ids = (rec_ids2).groupBy(0).reduceGroup(new uniqRecIds());

        DataSet<Tuple3<String, String, Boolean>> correctLinks_temp  = goldenTruth.leftOuterJoin(rec_ids).where(0).equalTo(0).with(new correctLinks());
        DataSet<Tuple2<String, String>> correctLinks1 = correctLinks_temp.flatMap(new filterCorrectLinks(true));
        DataSet<Tuple2<String, String>> correctLinks2 = correctLinks_temp.flatMap(new filterCorrectLinks(false)).join(rec_ids)
                .where(1).equalTo(0).with(new correctLinks()).flatMap(new filterCorrectLinks(true));
        DataSet<Tuple2<String, String>> correctLinks = correctLinks1.union(correctLinks2).map(new sortAndConcatTuple2()).distinct(0).map(new RichMapFunction<Tuple1<String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple1<String> value) throws Exception {
                return Tuple2.of(value.f0, "gt");
            }
        });
        DataSet<Tuple2<String, String>> allValidatedLinksRecIds =  allValidatedLinks.map(new MapFunction<Tuple5<String, String, Double, String, Boolean>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Tuple5<String, String, Double, String, Boolean> value) throws Exception {
                return Tuple2.of(value.f0+","+value.f1,"al");
            }
        });
        DataSet<Tuple2<String, String>> missedCorrectLinks = correctLinks.union(allValidatedLinksRecIds).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> in, Collector<Tuple2<String, String>> out) throws Exception {
                Tuple2<String, String> gt = new Tuple2<String, String>();
                Boolean isGt= false;
                int cnt = 0;
                for (Tuple2<String, String> i:in){
                    cnt++;
                    if (i.f1.equals("gt")) {
                        gt = Tuple2.of(i.f0.split(",")[0], i.f0.split(",")[1]);
                        isGt = true;
                    }
                }
                if (cnt<2 && isGt)
                    out.collect(gt);
            }
        });
        DataSet<Tuple2<String, String>> missedCorrectLinksWithGrdpids = missedCorrectLinks.join(gradoopId_recId).where(0).equalTo(1).with(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> join(Tuple2<String, String> in1, Tuple2<String, String> in2) throws Exception {
                return Tuple2.of(in2.f0, in1.f1);
            }
        }).join(gradoopId_recId).where(1).equalTo(1).with(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> join(Tuple2<String, String> in1, Tuple2<String, String> in2) throws Exception {
                return Tuple2.of(in1.f0, in2.f0);
            }
        });

        DataSet<Edge> newEdges = missedCorrectLinksWithGrdpids.map(new newLink2Edge(graph.getConfig().getEdgeFactory()));
        DataSet<Tuple2<String, String>> LGVerticesGrdpIds = LogicalGraphVertices.map(new MapFunction<Vertex, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Vertex value) throws Exception {
                return Tuple2.of(value.getId().toString(),"lg");
            }
        });
        DataSet<Tuple2<String, String>> newLinksGrdpIds = newEdges.flatMap(new FlatMapFunction<Edge, Tuple2<String, String>>() {
            @Override
            public void flatMap(Edge value, Collector<Tuple2<String, String>> out) throws Exception {
                out.collect(Tuple2.of(value.getSourceId().toString(),"gt"));
                out.collect(Tuple2.of(value.getTargetId().toString(),"gt"));
            }
        }).distinct(0);

        DataSet<Tuple2<Vertex, String>> vertex_gdId = clusteredGraphVertices.map(new MapFunction<Vertex, Tuple2<Vertex, String>>() {
            @Override
            public Tuple2<Vertex, String> map(Vertex in) throws Exception {
                return Tuple2.of(in, in.getId().toString());
            }
        });

        DataSet<Vertex> newVertices = LGVerticesGrdpIds.union(newLinksGrdpIds).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple1<String>>() {
            @Override
            public void reduce(Iterable<Tuple2<String, String>> in, Collector<Tuple1<String>> out) throws Exception {
                Tuple1<String> gt = new Tuple1<String>();
                Boolean isGt= false;
                int cnt = 0;
                for (Tuple2<String, String> i:in){
                    cnt++;
                    if (i.f1.equals("gt")) {
                        gt = Tuple1.of(i.f0);
                        isGt = true;
                    }
                }
                if (cnt<2 && isGt)
                    out.collect(gt);
            }
        }).join(vertex_gdId).where(0).equalTo(1).with(new JoinFunction<Tuple1<String>, Tuple2<Vertex, String>, Vertex>() {
            @Override
            public Vertex join(Tuple1<String> first, Tuple2<Vertex, String> second) throws Exception {
                second.f0.setProperty("IsMissing", true);
                return second.f0;
            }
        });

        return LogicalGraph.fromDataSets(LogicalGraphVertices.union(newVertices), LogicalGraphEdges.union(newEdges),
                graph.getConfig());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return ClusterIdNeighborSampling.class.getName();
    }
}
