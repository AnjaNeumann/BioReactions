package org.gradoop.flink.model.impl.operators.preprocess;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.vis.layout.*;
import org.gradoop.vis.layout.cose.CoSELayout;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

/**
 * Created by rostam on 09.06.17.
 *
 */
public class NeighborhoodGraph {

    HashMap<GradoopId,Vector<GradoopId>> graph = new HashMap<>();
    HashMap<GradoopId,Vertex> idToV = new HashMap<>();

    public NeighborhoodGraph(List<Vertex> vertices, List<Edge> edges) {
        for (Vertex v : vertices) {
            graph.put(v.getId(), new Vector<>());
            idToV.put(v.getId(), v);
        }
        for (Edge e : edges) {
            if (graph.get(e.getSourceId()) != null)
                graph.get(e.getSourceId()).add(e.getTargetId());
            if (graph.get(e.getTargetId()) != null)
                graph.get(e.getTargetId()).add(e.getSourceId());
        }
    }

    public Vector<GradoopId> neighbors(Vertex v) {
        return graph.get(v.getId());
    }

    public List<Vertex> filterDisconnectedVertices() {
        List<Vertex> ret = new Vector<>();
        for(GradoopId id : graph.keySet()) {
            if(graph.get(id).size() != 0) {
                ret.add(idToV.get(id));
            }
        }

        return ret;
    }

    public void onlyClusters() {
        HashMap<String,Integer> clusterId_Count = new HashMap<>();
        for(Vertex v : idToV.values()) {
            String clusterId = v.getProperties().get("ClusterId").toString();
            if(clusterId_Count.get(clusterId) == null) {
                clusterId_Count.put(clusterId,1);
            } else {
                clusterId_Count.put(clusterId,clusterId_Count.get(clusterId)+1);
            }
        }

        Vector<String> clusterIdsWithMoreThanOneElement = new Vector<>();
        for(String s : clusterId_Count.keySet()) {
            if(clusterId_Count.get(s) > 1) {
                clusterIdsWithMoreThanOneElement.add(s);
            }
        }

        List<Vertex> selectedVertex = new Vector<>();
        for(Vertex v : idToV.values()) {
            String clusterId = v.getProperties().get("ClusterId").toString();
            if(clusterIdsWithMoreThanOneElement.contains(clusterId)) {
                selectedVertex.add(v);
                v.setProperty("parent",clusterId);
            }
        }
    }

//    public void forceDirected(List<Vertex> vertices, List<Edge> edges, int maxIter) {
//        edu.uci.ics.jung.graph.Graph<String,String> jungG = new DirectedSparseGraph<String,String>();
//        for(Vertex v : vertices) {
//            jungG.addVertex(v.getId().toString());
//        }
//        //jungG.addVertex("Test");
//        for(Edge e : edges) {
//            jungG.addEdge(e.getSourceId().toString()+"->"+e.getTargetId().toString(),e.getSourceId().toString(),e.getTargetId().toString());
//       //     jungG.addEdge(e.getSourceId().toString(),e.getSourceId().toString(),"Test");
//        }
//        KKLayout<String,String> kk = new KKLayout<String, String>(jungG);
//        kk.initialize();
//        kk.setSize(new Dimension(600,400));
////        for(int i=0;i<maxIter;i++) {
////            kk.step();
////            if(kk.done()) break;
////        }
//        FRLayout<String,String> fr = new FRLayout<String, String>(jungG, new Dimension(700,400));
//        fr.setRepulsionMultiplier(0.9);
////        fr.setInitializer(new Function<String, Point2D>() {
////            @Nullable
////            @Override
////            public Point2D apply(@Nullable String s) {
////                return new Point2D.Double(kk.getX(s),kk.getY(s));
////            }
////        });
//        fr.initialize();
//        for(int i=0;i<maxIter;i++) {
//            fr.step();
//            System.out.println(i);
//            if(fr.done()) break;
//        }
//        for(Vertex v : idToV.values()) {
//            v.setProperty("position",fr.getX(v.getId().toString())+","+fr.getY(v.getId().toString()));
//        }
//    }

    public void forceDirected2(List<Vertex> vertices, List<Edge> edges, int maxIter) {
        Layout layout = new CoSELayout();
        LGraphManager gm = layout.getGraphManager();
        LGraph g1 = gm.addRoot();

        HashMap<GradoopId,LNode> hm = new HashMap<>();
        HashMap<GradoopId,Vertex> hm2 = new HashMap<>();
        for(Vertex v : vertices) {
            LNode n = layout.newNode(null);
            n.getRect().setWidth(50);
            n.getRect().setHeight(50);
            g1.add(n);
            n.label = v.getId().toString();
            hm.put(v.getId(),n);
            hm2.put(v.getId(),v);
        }
        for(Edge e : edges) {
            LEdge le = layout.newEdge(null);
                try {
                    if(e!=null && le != null)
                        if(hm.get(e.getSourceId()) != null && hm.get(e.getTargetId())!= null)
                    g1.add(le, hm.get(e.getSourceId()), hm.get(e.getTargetId()));
                } catch (NullPointerException ee) {
                    System.out.println(le);
                    System.out.println(e);
                    System.out.printf(hm.get(e.getSourceId()).toString());
                    System.out.printf(hm.get(e.getTargetId()).toString());
                }
        }
        layout.runLayout();

        for(Object o : layout.getAllNodes()) {
            LNode n = (LNode) o;
            hm2.get(GradoopId.fromString(n.label)).setProperty("position",n.getRect().getX()+","+n.getRect().getY());
        }
    }

    public Vector<String> unqiueClusterIds(List<Vertex> lv) {
        Vector<String> ret = new Vector<>();
        for(Vertex v : lv) {
            String ClusterId = v.getPropertyValue("ClusterId").toString();
            if(ClusterId.contains(",")) {
                ClusterId = ClusterId.substring(0,ClusterId.indexOf(","));
            }
            if(!ret.contains(ClusterId)) {
                ret.add(ClusterId);
            }
        }
        return ret;
    }

    public void forceDirectedCluster(List<Vertex> vertices, List<Edge> edges, int maxIter) {
        Vector<String> uniqueCID = unqiueClusterIds(vertices);
        Layout layout = new CoSELayout();
        LGraphManager gm = layout.getGraphManager();
        LGraph root = gm.addRoot();
        HashMap<String, LGraph> ClusterIdToChildGraph = new HashMap<>();

        HashMap<String,String> clusterIdToColor = new HashMap<>();
        int i = 0;
        for(String id : uniqueCID) {
            LNode n = root.add(layout.newNode(null));
            n.label = "parent";
            LGraph childGraph = gm.add(layout.newGraph(null), n);
            ClusterIdToChildGraph.put(id,childGraph);
            if(i< DistinctColors.indexcolors.length) clusterIdToColor.put(id, DistinctColors.indexcolors[i]);
            else clusterIdToColor.put(id, DistinctColors.indexcolors[i-DistinctColors.indexcolors.length]);
            i++;
        }

        HashMap<GradoopId,LNode> hm = new HashMap<>();
        HashMap<GradoopId,Vertex> hm2 = new HashMap<>();
        for(Vertex v : vertices) {
            LNode n = layout.newNode(null);
            n.getRect().setWidth(50);
            n.getRect().setHeight(50);
            n.label = v.getId().toString();
            String ClusterId = v.getPropertyValue("ClusterId").toString();
            if(ClusterId.contains(","))
                ClusterId = ClusterId.substring(0,ClusterId.indexOf(","));
            ClusterIdToChildGraph.get(ClusterId).add(n);
            hm.put(v.getId(),n);
            hm2.put(v.getId(),v);
        }

        for(Edge e : edges) {
            LEdge le = layout.newEdge(null);
            try {
                if(e!=null && le != null)
                    if(hm.get(e.getSourceId()) != null && hm.get(e.getTargetId())!= null) {
                        Vertex src = hm2.get(e.getSourceId());
                        Vertex tgt = hm2.get(e.getTargetId());
                        String srcClusterId = src.getPropertyValue("ClusterId").toString();
                        String tgtClusterId = tgt.getPropertyValue("ClusterId").toString();
                        if(srcClusterId.contains(","))
                            srcClusterId = srcClusterId.substring(0,srcClusterId.indexOf(","));
                        if(tgtClusterId.contains(","))
                            tgtClusterId = tgtClusterId.substring(0,tgtClusterId.indexOf(","));
                        if(srcClusterId.equals(tgtClusterId)) {
                            ClusterIdToChildGraph.get(srcClusterId)
                                    .add(le, hm.get(e.getSourceId()), hm.get(e.getTargetId()));
                        } else {
                            root.add(le, hm.get(e.getSourceId()), hm.get(e.getTargetId()));
                        }
                    }
            } catch (NullPointerException ee) {
                System.out.println(le);
                System.out.println(e);
                System.out.printf(hm.get(e.getSourceId()).toString());
                System.out.printf(hm.get(e.getTargetId()).toString());
            }
        }
        layout.runLayout();

        for(Object o : layout.getAllNodes()) {
            LNode n = (LNode) o;
            if(!n.label.equals("parent")) {
                Vertex v = hm2.get(GradoopId.fromString(n.label));
                v.setProperty("position", n.getRect().getX() + "," + n.getRect().getY());
                String ClusterId = v.getPropertyValue("ClusterId").toString();
                if(ClusterId.contains(","))
                    ClusterId = ClusterId.substring(0,ClusterId.indexOf(","));
                v.setProperty("color",clusterIdToColor.get(ClusterId));
            }
        }
    }
}
