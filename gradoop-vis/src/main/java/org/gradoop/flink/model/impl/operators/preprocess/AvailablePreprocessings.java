package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.gradoop.flink.model.impl.operators.preprocess.randomWalkWithRestart.RandomWalk;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.preprocess.clustering.ClusterIdNeighborSampling;
import org.gradoop.vis.RequestHandler;

import java.util.Comparator;
import java.util.List;
import java.util.Vector;

/**
 * Created by rostam on 22.06.17.
 * Available samplings
 */
public class AvailablePreprocessings {
    public final static String NoSampling = "No Sampling";
    public final static String NodeSampling = "Node Sampling";
    public final static String EdgeSampling = "Edge Sampling";
    public final static String PageRankSampling = "PageRank Sampling";
    public final static String RandomWalkSampling = "Random Walk Sampling";
    public final static String ClustersSampling = "Cluster Size Sampling";
    public final static String ClusterNeighborSampling = "Cluster Neighbor Sampling";
    public final static String GraphSummary = "Graph Summary";
    public final static String GraphClustering = "Simple Graph Clustering";

    public JSONArray getAvailablePreprocessings() {
        JSONArray jsonArray = new JSONArray();
        jsonArray.put(NoSampling);
        jsonArray.put(NodeSampling);
        jsonArray.put(EdgeSampling);
        jsonArray.put(PageRankSampling);
        jsonArray.put(RandomWalkSampling);
        jsonArray.put(ClustersSampling);
        jsonArray.put(ClusterNeighborSampling);
        jsonArray.put(GraphSummary);
        jsonArray.put(GraphClustering);
        return jsonArray;
    }

    public Vector<String> getAvailablePreprocessingsAsVector() {
        Vector<String> res = new Vector<>();
        JSONArray jsonArray = getAvailablePreprocessings();
        try {
            for (int i = 0; i < jsonArray.length(); i++) {
                res.add(jsonArray.get(i).toString());
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        return res;
    }

    public LogicalGraph sample(LogicalGraph graph, String sampling, float samplingThreshold,
                               Vector<Pair<String,String>> hm, int min, int max, String clusterId, ExecutionEnvironment env) {
        switch (sampling) {
            case NodeSampling:
                graph = graph.sampleRandomNodes(samplingThreshold);
                break;
            case EdgeSampling:
                graph = new RandomEdgeSampling(samplingThreshold).execute(graph);
                break;
            case RandomWalkSampling:
                RandomWalk rw = new RandomWalk();
                break;
            case PageRankSampling:
                org.gradoop.flink.model.impl.operators.preprocess.PageRankSampling prs
                        = new PageRankSampling( 0.5,samplingThreshold);
                try {
                    graph = prs.sample(graph);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case ClusterNeighborSampling:
                String dataPath = RequestHandler.class.getResource("/data/pm.csv").getFile();
                ClusterIdNeighborSampling cins = new ClusterIdNeighborSampling(clusterId,env,dataPath);
                graph=cins.execute(graph);
                graph=new org.gradoop.flink.model.impl.operators.preprocess.RandomNodeSampling(1).execute(graph);
                break;
            case ClustersSampling:
                org.gradoop.flink.model.impl.operators.preprocess.ClustersSampling cs = new ClustersSampling(min,max);
                graph = cs.execute(graph);
                List<Tuple2<Integer, Integer>> lt = null;
                try {
                    lt = cs.getMapOfNumOfClusters().map(new MapFunction<Tuple2<String,Integer>, Tuple2<Integer,Integer>>() {
                        @Override
                        public Tuple2<Integer, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                            return new Tuple2<>(stringIntegerTuple2.f1,1);
                        }
                    }).groupBy(0).sum(1).collect();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String key = "The number of cluster with sizes ";
                String val = "";
                lt.sort(new Comparator<Tuple2<Integer, Integer>>() {
                    @Override
                    public int compare(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
                        return t1.f0 - t2.f0;
                    }
                });
                for(Tuple2<Integer, Integer> t : lt) {
                    key+=t.f0+", ";
                    val+=t.f1+", ";
                }
                key = key.substring(0,key.length()-2);
                val = val.substring(0,val.length()-2);
                hm.add(new Pair<>(key,val));
                //ret = cs.getMapOfNumOfClusters().collect();
            case GraphSummary:
                graph = new GraphSummary(1d,1d).execute(graph);
            case GraphClustering:
                graph = new SimpleGraphClustering(1d, 1d).execute(graph);
            default:
                break;
        }
        return graph;
    }
}