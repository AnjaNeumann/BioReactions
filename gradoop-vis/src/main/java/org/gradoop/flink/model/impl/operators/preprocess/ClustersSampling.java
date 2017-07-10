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

package org.gradoop.flink.model.impl.operators.preprocess;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;

/**
 * Takes a logical graph and a user defined aggregate function as input. The
 * aggregate function is applied on the logical graph and the resulting
 * aggregate is stored as an additional property at the result graph.
 */
public class ClustersSampling implements UnaryGraphToGraphOperator {
    private DataSet<Tuple2<String, Integer>> ClusterIdNumber;
    private int min, max;

    /**
     * Creates new RandomNodeSampling instance.
     *
     * @param min minimum cluster size
     * @param max maximum cluster size
     */
    public ClustersSampling(int min, int max) {
        this.min = min;
        this.max = max;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalGraph execute(LogicalGraph graph) {

        ClusterSizeFilterFunction csff = new ClusterSizeFilterFunction(min, max);
        ClusterIdNumber = graph.getVertices().map(new MapFunction<Vertex, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Vertex vertex) throws Exception {
                return new Tuple2<String, Integer>(vertex.getPropertyValue("ClusterId").toString(), 1);
            }
        }).groupBy(0).sum(1).filter(csff);

        DataSet<Vertex> newVertices = graph.getVertices().join(ClusterIdNumber).where(new KeySelector<Vertex, String>() {
            @Override
            public String getKey(Vertex vertex) throws Exception {
                return vertex.getProperties().get("ClusterId").toString();
            }
        }).equalTo(0).with(new LeftSide<>()).map(new MapFunction<Vertex, Vertex>() {
            @Override
            public Vertex map(Vertex vertex) throws Exception {
                vertex.setLabel(vertex.getProperties().get("ClusterId").toString());
                return vertex;
            }
        });

        DataSet<Edge> newEdges = graph.getEdges()
                .join(newVertices)
                .where(new SourceId<>())
                .equalTo(new Id<Vertex>())
                .with(new LeftSide<Edge, Vertex>())
                .join(newVertices)
                .where(new TargetId<>())
                .equalTo(new Id<Vertex>())
                .with(new LeftSide<Edge, Vertex>());

        return LogicalGraph.fromDataSets(
                newVertices, newEdges, graph.getConfig());
    }

    public DataSet<Tuple2<String, Integer>> getMapOfNumOfClusters() {
        return ClusterIdNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getName() {
        return ClustersSampling.class.getName();
    }
}
