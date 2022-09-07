package org.apache.flink.streaming.api.functions.sketch;


import org.apache.datasketches.hll.HllSketch;

import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.SketchAggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.typeutils.FieldAccessor;
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory;

import static org.apache.datasketches.hll.HllSketch.DEFAULT_LG_K;

@Internal
public class HllAccumulator<IN> extends SketchAggregateFunction<IN, HllSketch, Double> {

    private static final long serialVersionUID = 1L;

    private final int lgConfigK;
    private final TgtHllType tgtHllType;
    private final FieldAccessor<IN, Object> fieldAccessor;
    private final HllFunction adder;

    public HllAccumulator(int pos, TypeInformation<IN> typeInfo, ExecutionConfig config) {
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        adder = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
        this.lgConfigK = DEFAULT_LG_K;
        this.tgtHllType = TgtHllType.HLL_4;
    }

    public HllAccumulator(
            int pos,
            TypeInformation<IN> typeInfo,
            ExecutionConfig config,
            int lgConfigK,
            TgtHllType tgtHllType) {
        fieldAccessor = FieldAccessorFactory.getAccessor(typeInfo, pos, config);
        adder = HllFunction.getForClass(fieldAccessor.getFieldType().getTypeClass());
        // TODO: lgConfigK 和 tgtHllType参数校验
        this.lgConfigK = lgConfigK;
        this.tgtHllType = tgtHllType;
    }

    @Override
    public HllSketch createAccumulator() {
        return new HllSketch(lgConfigK, tgtHllType);
    }

    @Override
    public HllSketch add(IN value, HllSketch accumulator) {
        adder.add(accumulator, fieldAccessor.get(value));
        return accumulator;
    }

    @Override
    public Double getResult(HllSketch accumulator) {
        return accumulator.getEstimate();
    }

    @Override
    public HllSketch merge(HllSketch a, HllSketch b) {
        Union union = new Union();
        union.update(a);
        union.update(b);
        return union.getResult();
    }
}
