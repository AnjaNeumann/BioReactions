package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;

import java.io.Serializable;

/**
 * Created by alieh on 6/21/17.
 */
public class link2Edge implements MapFunction <Tuple5<String, String, Double, String, Boolean>, Edge>, Serializable {
    private final EdgeFactory ef;
    public link2Edge (EdgeFactory EdgeFactory){ ef = EdgeFactory;}
    @Override
    public Edge map(Tuple5<String, String, Double, String, Boolean> in) throws Exception {
        Edge edge = ef.createEdge(new GradoopId().fromString(in.f0), new GradoopId().fromString(in.f1));
        edge.setProperty("value",in.f2);
        edge.setProperty("vis_label", (float) Math.round(in.f2 * 100) / 100);
        edge.setProperty("IsCorrect", in.f4);
        edge.setProperty("IsMissing", false);

        return edge;
    }
}
