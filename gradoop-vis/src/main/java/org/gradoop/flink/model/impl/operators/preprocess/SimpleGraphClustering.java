package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.Summarization;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.labelpropagation.GellyLabelPropagation;
import org.gradoop.flink.algorithms.labelpropagation.GradoopLabelPropagation;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

import java.util.ArrayList;
import java.util.List;

public class SimpleGraphClustering implements UnaryGraphToGraphOperator {
    private final double threshold;
    private double dampeningFactor;

    public SimpleGraphClustering(double dampeningFactor, double threshold) {
        this.dampeningFactor = dampeningFactor;
        this.threshold = threshold;
    }

    @Override
    public LogicalGraph execute(LogicalGraph graph) {
        // property key used for label propagation
        final String communityKey = "value";

        // load the graph and set initial community id
        graph = graph.transformVertices((current, transformed) -> {
            current.setProperty(communityKey, current.getId().toString());
            return current;
        });

        graph = graph.callForGraph(new GradoopLabelPropagation(10, communityKey));
        Grouping.GroupingBuilder builder = new Grouping.GroupingBuilder();
        builder.addVertexGroupingKey(communityKey);
        builder.setStrategy(GroupingStrategy.GROUP_REDUCE);
        //builder.addVertexAggregator(new CountAggregator());
        builder.addVertexAggregator(new KeepPropertyAggregator());
        graph = builder.build().execute(graph);
        return graph;
    }

    @Override
    public String getName() {
        return SimpleGraphClustering.class.getName();
    }
}
