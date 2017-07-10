package org.gradoop.flink.model.impl.operators.preprocess.clustering;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;

import java.io.Serializable;

/**
 * Created by alieh on 6/21/17.
 */
public class newLink2Edge implements MapFunction<Tuple2<String, String>, Edge>, Serializable {

    private final EdgeFactory ef;
    public newLink2Edge (EdgeFactory EdgeFactory){ ef = EdgeFactory;}
    @Override
    public Edge map(Tuple2<String, String> in) throws Exception {
        Edge edge = ef.createEdge(new GradoopId().fromString(in.f0), new GradoopId().fromString(in.f1));
        edge.setProperty("value","?");
        edge.setProperty("vis_label", "");
        edge.setProperty("IsCorrect", true);
        edge.setProperty("IsMissing", true);
        return edge;
    }
}
