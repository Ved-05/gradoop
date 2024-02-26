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

import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Maps Gradoop Vertex to a Gelly vertex with the {@link GradoopId} as its id.
 * identifier and {@link Double} as edge value.
 *
 * @param <V> Gradoop Vertex type
 */
public class GradoopVertexToJellyVertex<V extends Vertex> implements VertexToGellyVertex<V, Tuple2<Long, Long>> {

    /**
     * Reduce object instantiations
     */
    private final org.apache.flink.graph.Vertex<GradoopId, Tuple2<Long, Long>> reuseVertex;

    /**
     * Constructor.
     */
    public GradoopVertexToJellyVertex() {
        reuseVertex = new org.apache.flink.graph.Vertex<>();
    }

    @Override
    public org.apache.flink.graph.Vertex<GradoopId, Tuple2<Long, Long>> map(V vertex) {
        TemporalVertex temporalVertex = (TemporalVertex) vertex;
        reuseVertex.setId(temporalVertex.getId());
        reuseVertex.setValue(new Tuple2<>(temporalVertex.getValidFrom(), temporalVertex.getValidTo()));
        return reuseVertex;
    }
}
