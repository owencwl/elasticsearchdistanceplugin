package com.umxwe.elasticsearch.plugin.distance;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName InternalUmxDistance
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class InternalUmxDistance extends InternalNumericMetricsAggregation.SingleValue implements UmxDistance {
    private final static Logger logger = LoggerFactory.getLogger(InternalUmxDistance.class);

    private final UmxSpeedCompute speedCompute;

    private  final double result;
    private  final long count;


    public InternalUmxDistance(StreamInput in) throws IOException {
        super(in);
        speedCompute = in.readOptionalWriteable(UmxSpeedCompute::new);
        result = in.readDouble();
        count = in.readVLong();


    }

    public InternalUmxDistance(String name,long count, double result, UmxSpeedCompute umxSpeedComputeResults, Map<String, Object> metadata) {
        super(name, metadata);
        this.speedCompute = umxSpeedComputeResults;
        this.result = result;
        this.count = count;
    }


    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(result);
        out.writeVLong(count);
    }

    /**
     * reduce计算逻辑，或运算
     *
     * @param aggregations
     * @param reduceContext
     * @return
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        UmxSpeedCompute umxSpeedCompute = new UmxSpeedCompute();

        for (InternalAggregation aggregation : aggregations) {
            UmxSpeedCompute value = ((InternalUmxDistance) aggregation).speedCompute;
            umxSpeedCompute.merge(value);
        }
        if (reduceContext.isFinalReduce()) {
            logger.info("InternalUmxDistance_isFinalReduce:{}",reduceContext.isFinalReduce());
            return new InternalUmxDistance(name, umxSpeedCompute.docCount,umxSpeedCompute.getMaxSpeed(), umxSpeedCompute, getMetadata());
        }
        return new InternalUmxDistance(name,0, 0.0, umxSpeedCompute, getMetadata());
    }

    /**
     * 计算结果进行返回
     *
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {


        /**
         * eg:
         *   "speed" :
         *   {
         *        "doc_count" : 1,
         *        "maxspeed" : 3.0887521601880075E-7
         *    }
         */
        builder.field(CommonFields.DOC_COUNT.getPreferredName(),count);
        builder.field("maxspeed", result);


        return builder;
    }

    @Override
    public String getWriteableName() {
        return UmxDistanceAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return result;
    }

    @Override
    public double getValue() {
        return result;
    }
}
