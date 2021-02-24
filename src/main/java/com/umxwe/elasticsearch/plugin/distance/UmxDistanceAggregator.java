package com.umxwe.elasticsearch.plugin.distance;

import com.alibaba.fastjson.JSON;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName UmxDistanceAggregator
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class UmxDistanceAggregator extends MetricsAggregator {
    private final static Logger logger = LoggerFactory.getLogger(UmxDistanceAggregator.class);

    private final MultArrayValuesSource.MapArrayValuesSource valuesSources;

    ObjectArray<UmxSpeedCompute> speeds;

    public UmxDistanceAggregator(String name,  Map<String, ValuesSource> valuesSources, SearchContext searchContext, Aggregator aggregator, MultiValueMode multiValueMode, Map<String, Object> stringObjectMap) throws IOException {
        super(name, searchContext, aggregator, stringObjectMap);
        if (valuesSources != null && !valuesSources.isEmpty()) {
            this.valuesSources = new MultArrayValuesSource.MapArrayValuesSource(valuesSources,multiValueMode);
            logger.info("UmxDistanceAggregator_values1_size:{},values2_size:{}", this.valuesSources.values1.length,this.valuesSources.values2.length);
        } else {
            this.valuesSources = null;
        }
    }


    /**
     * buildAggrgation方法则会将收集好的结果进行处理
     * @param bucket
     * @return
     * @throws IOException
     */
    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (valuesSources == null || bucket >= speeds.size()) {
            return buildEmptyAggregation();
        }
        final UmxSpeedCompute speed = speeds.get(bucket);
        return new InternalUmxDistance(name, speeds.size(), speed, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalUmxDistance(name,0, null, metadata());
    }

    /**
     * 是获取Collector,其实是一个迭代器，迭代所有文档，在这个步骤中，我们会获取一个Collector,然后依托于DocValues进行计数。
     * @param ctx
     * @param sub
     * @return
     * @throws IOException
     */
    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSources == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final NumericDoubleValues[] values1 = new NumericDoubleValues[valuesSources.fieldNames().length];
        final MultiGeoPointValues[] values2 = new MultiGeoPointValues[valuesSources.fieldNames().length];
        for (int i = 0; i < values1.length; ++i) {
            values1[i] = valuesSources.getField(i, ctx).v1();
            values2[i] = valuesSources.getField(i, ctx).v2();
        }
        logger.info("fieldNames:{}",valuesSources.fieldNames());


        Tuple values= new Tuple<>(values1,values2);
//        GeoDistance.ARC.calculate();
        //valuesSources 转为 number and  geopoint类型
        return new LeafBucketCollectorBase(sub,values){
            final String[] fieldNames = valuesSources.fieldNames();

            final double[] field1Vals = new double[fieldNames.length];
            final GeoPoint[] field2Vals = new GeoPoint[fieldNames.length];

            @Override
            public void collect(int doc, long bucket) throws IOException {
                // get fields
                if (includeDocument(doc)) {
                    speeds = bigArrays.grow(speeds, bucket + 1);
                    UmxSpeedCompute speed = speeds.get(bucket);
                    // add document fields to correlation stats
                    if (speed == null) {
                        speed = new UmxSpeedCompute(fieldNames, field1Vals,field2Vals);
                        speeds.set(bucket, speed);
                    } else {
                        speed.add(fieldNames, field1Vals,field2Vals);
                    }
                }
            }
            /**
             * return a map of field names and data
             */
            private boolean includeDocument(int doc) throws IOException {
                // loop over fields
                for (int i = 0; i < field1Vals.length; ++i) {
                    final NumericDoubleValues doubleValues = values1[i];
                    final int valuesCount = values2[i].docValueCount();

                    final MultiGeoPointValues geoPointValues = values2[i];
                    if (doubleValues.advanceExact(doc) && geoPointValues.advanceExact(doc)) {
                        final double value1 = doubleValues.doubleValue();
                        logger.info("value1:{}",value1);
                        final GeoPoint value2 = geoPointValues.nextValue();
                        if (value1 == Double.NEGATIVE_INFINITY) {
                            // TODO: Fix matrix stats to treat neg inf as any other value
                            return false;
                        }
                        field1Vals[i] = value1;
                        field2Vals[i] = value2;
                    } else {
                        return false;
                    }
                }
                return true;
            }
        };
    }


    @Override
    public ScoreMode scoreMode() {
        return valuesSources != null && valuesSources.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public void close() {
        Releasables.close(speeds);
    }
}
