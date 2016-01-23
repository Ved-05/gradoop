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

package org.gradoop.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (f0,f1) => (f0)
 *
 * @param <T0> f0 type
 * @param <T1> f1 type
 */
@FunctionAnnotation.ForwardedFields("f0")
public class Project2To0<T0, T1>
  implements MapFunction<Tuple2<T0, T1>, Tuple1<T0>> {

  /**
   * Reduce instantiations
   */
  private final Tuple1<T0> reuseTuple = new Tuple1<>();

  @Override
  public Tuple1<T0> map(Tuple2<T0, T1> pair) throws Exception {
    reuseTuple.f0 = pair.f0;
    return reuseTuple;
  }
}
