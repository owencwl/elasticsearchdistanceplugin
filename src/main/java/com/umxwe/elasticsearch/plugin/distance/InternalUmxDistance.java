package com.umxwe.elasticsearch.plugin.distance;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
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
public class InternalUmxDistance extends InternalNumericMetricsAggregation.SingleValue implements UmxDistance{
    private final static Logger logger = LoggerFactory.getLogger(InternalUmxDistance.class);

    private  int speedFlag=0;


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
        out.writeInt(speedFlag);
    }

    /**
     * reduce计算逻辑，或运算
     * @param aggregations
     * @param reduceContext
     * @return
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
       logger.info("InternalUmxDistance_reduce");
       return null;

//        // merge stats across all shards
//        List<InternalAggregation> aggs = new ArrayList<>(aggregations);
//        aggs.removeIf(p -> ((InternalUmxDistance)p).speedFlag == null);
//
//        // return empty result iff all stats are null
//        if (aggs.isEmpty()) {
//            return new InternalMatrixStats(name, 0, null, new MatrixStatsResults(), getMetadata());
//        }
//
//        RunningStats runningStats = new RunningStats();
//        for (InternalAggregation agg : aggs) {
//            runningStats.merge(((InternalMatrixStats) agg).stats);
//        }
//
//        if (reduceContext.isFinalReduce()) {
//            MatrixStatsResults results = new MatrixStatsResults(runningStats);
//            return new InternalMatrixStats(name, results.getDocCount(), runningStats, results, getMetadata());
//        }
//        return new InternalMatrixStats(name, runningStats.docCount, runningStats, null, getMetadata());
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
        boolean hasValue = !Double.isInfinite(speedFlag);
        /**
         * eg:
         *   "aggregations" : {
         *     "plateNo_count" : {
         *       "value" : 185722
         *     }
         *   }
         */
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? speedFlag : null);
        //转换字符串格式
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(speedFlag).toString());
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return UmxDistanceAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return speedFlag;
    }

    @Override
    public double getValue() {
        return speedFlag;
    }
}
