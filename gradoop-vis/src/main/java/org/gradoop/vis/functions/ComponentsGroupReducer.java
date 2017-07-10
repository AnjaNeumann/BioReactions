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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.vis.functions;


import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

/**
 * Reduce the Dataset of properties, represented as tuples3 of label of vertices with this
 * property, property key and a boolean specifying if it is numerical into one tuple3 with all
 * vertex labels in the first field.
 */
public class ComponentsGroupReducer implements GroupReduceFunction<
        Tuple2<Integer, GradoopId>, Tuple2<Integer, Vector<GradoopId>>> {

  @Override
  public void reduce(Iterable<Tuple2<Integer, GradoopId>> iterable, Collector<Tuple2<Integer, Vector<GradoopId>>> collector) throws Exception {
    Tuple2<Integer,Vector<GradoopId>> ress = new Tuple2<>();
    ress.f1 = new Vector<>();
    for(Tuple2<Integer, GradoopId> tuple : iterable) {
      ress.f0 = tuple.f0;
      ress.f1.add(tuple.f1);
    }
    collector.collect(ress);
  }
}
