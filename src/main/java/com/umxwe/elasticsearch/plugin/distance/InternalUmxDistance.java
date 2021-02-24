package com.umxwe.elasticsearch.plugin.distance;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @ClassName InternalUmxDistance
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class InternalUmxDistance extends InternalNumericMetricsAggregation.SingleValue implements UmxDistance{

    private double speed;


    public InternalUmxDistance(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
    }

    public InternalUmxDistance(String name,long count, UmxSpeedCompute umxSpeedComputeResults, Map<String, Object> metadata) {
        super(name,metadata);

    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(speed);
    }

    /**
     * reduce计算逻辑，主要实现聚合的业务逻辑
     * @param aggregations
     * @param reduceContext
     * @return
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        return null;
    }

    /**
     * 计算结果进行返回
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !Double.isInfinite(speed);
        /**
         * eg:
         *   "aggregations" : {
         *     "plateNo_count" : {
         *       "value" : 185722
         *     }
         *   }
         */
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? speed : null);
        //转换字符串格式
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(speed).toString());
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return UmxDistanceAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return speed;
    }

    @Override
    public double getValue() {
        return speed;
    }
}
