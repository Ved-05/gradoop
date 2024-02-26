/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.algorithms.common;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;

/**
 * Maps Gradoop edge to a Gelly edge consisting of Gradoop source and target
 * identifier and {@link Double} as edge value.
 *
 * @param <E> Gradoop edge type.
 */
public class GradoopEdgeToGellyEdge<E extends Edge> implements EdgeToGellyEdge<E, Tuple3<Integer, Long, Long>> {

  /**
   * Reduce object instantiations.
   */
  private final org.apache.flink.graph.Edge<GradoopId, Tuple3<Integer, Long, Long>> jellyEdge;

  /**
   * Constructor.
   *
   */
  public GradoopEdgeToGellyEdge() {
    this.jellyEdge = new org.apache.flink.graph.Edge<>();
  }

  @Override
  public org.apache.flink.graph.Edge<GradoopId, Tuple3<Integer, Long, Long>> map(E gradoopEdge) {
    TemporalEdge temporalEdge = (TemporalEdge) gradoopEdge;
    jellyEdge.setSource(temporalEdge.getSourceId());
    jellyEdge.setTarget(temporalEdge.getTargetId());
    jellyEdge.setValue(new Tuple3<>(1, temporalEdge.getValidFrom(), temporalEdge.getValidTo()));
    return jellyEdge;
  }
}
