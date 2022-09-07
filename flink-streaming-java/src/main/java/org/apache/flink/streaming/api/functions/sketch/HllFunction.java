/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sketch;

import org.apache.datasketches.hll.HllSketch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.aggregation.SumAggregator;

import java.io.Serializable;
import java.nio.ByteBuffer;

/** Internal function for summing up contents of fields. This is used with {@link SumAggregator}. */
@Internal
public abstract class HllFunction implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract void add(HllSketch hllSketch, Object o);

    public static HllFunction getForClass(Class<?> clazz) {

        if (clazz == Long.class) {
            return new LongHll();
        } else if (clazz == Double.class) {
            return new DoubleHll();
        } else if (clazz == String.class) {
            return new StringHll();
        } else if (clazz == ByteBuffer.class) {
            return new ByteBufferHll();
        } else if (clazz == int[].class) {
            return new IntArrayHll();
        } else if (clazz == long[].class) {
            return new LongArrayHll();
        } else if (clazz == char[].class) {
            return new CharArrayHll();
        } else if (clazz == byte[].class) {
            return new ByteArrayHll();
        } else {
            throw new RuntimeException(
                    "DataStream cannot be HllSketch because the class "
                            + clazz.getSimpleName()
                            + " does not support the + operator.");
        }
    }

    static class LongHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((long) o);
        }
    }

    static class DoubleHll extends HllFunction {

        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((double) o);
        }
    }

    static class StringHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((String) o);
        }
    }

    static class ByteBufferHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((ByteBuffer) o);
        }
    }

    static class IntArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((int[]) o);
        }
    }

    static class LongArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((long[]) o);
        }
    }

    static class CharArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((char[]) o);
        }
    }

    static class ByteArrayHll extends HllFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void add(HllSketch hllSketch, Object o) {
            hllSketch.update((byte[]) o);
        }
    }


}
