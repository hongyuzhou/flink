package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Internal;

@Internal
public abstract class SketchAggregateFunction<IN, ACC, OUT> implements AggregateFunction<IN, ACC, OUT> {

    private static final long serialVersionUID = 1L;

    public enum SketchAccumulatorType {
        HLL,
        CPC
    }
}
