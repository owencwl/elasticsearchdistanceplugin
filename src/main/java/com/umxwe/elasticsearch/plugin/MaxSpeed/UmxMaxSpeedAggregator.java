package com.umxwe.elasticsearch.plugin.MaxSpeed;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
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
public class UmxMaxSpeedAggregator extends MetricsAggregator {
    private final static Logger logger = LoggerFactory.getLogger(UmxMaxSpeedAggregator.class);

    private final ArrayValuesSource.MultArrayValuesSource valuesSources;

    ObjectArray<UmxSpeedCompute> speeds;

    /**
     * 构造函数初始化
     *
     * @param name
     * @param valuesSources
     * @param searchContext
     * @param aggregator
     * @param stringObjectMap
     * @throws IOException
     */
    public UmxMaxSpeedAggregator(String name, Map<String, ValuesSource> valuesSources, SearchContext searchContext, Aggregator aggregator, Map<String, Object> stringObjectMap) throws IOException {
        super(name, searchContext, aggregator, stringObjectMap);
        /**
         * 初始化 valuesSources 和 speeds
         */
        if (valuesSources != null && !valuesSources.isEmpty()) {
            this.valuesSources = new ArrayValuesSource.MultArrayValuesSource(valuesSources);
            speeds = context.bigArrays().newObjectArray(1);
        } else {
            this.valuesSources = null;
        }
    }


    /**
     * buildAggrgation方法则会将收集好的结果进行处理
     *
     * @param bucket
     * @return
     * @throws IOException
     */
    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (valuesSources == null || bucket >= speeds.size()) {
            return buildEmptyAggregation();
        }
        //每个bucket的计算结果对象
        final UmxSpeedCompute speed = speeds.get(bucket);
        return new InternalUmxMaxSpeed(name, 0, 0, speed, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalUmxMaxSpeed(name, 0, 0, null, metadata());
    }

    /**
     * 是获取Collector,其实是一个迭代器，迭代所有文档，在这个步骤中，我们会获取一个Collector,然后依托于DocValues进行计数。
     *
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

        SortedNumericDoubleValues values1 = valuesSources.getValues1().doubleValues(ctx);
        MultiGeoPointValues values2 = valuesSources.getValues2().geoPointValues(ctx);
        logger.info("fieldNames:{},number_docValueCount:{},geo_docValueCount:{}", valuesSources.fieldNames(), values1.docValueCount(), values2.docValueCount());
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                logger.info("doc:{},bucket:{}", doc, bucket);

                if (values1.advanceExact(doc) && values2.advanceExact(doc)) {
                    speeds = bigArrays.grow(speeds, bucket + 1);

                    for (int i = 0; i < values1.docValueCount(); i++) {
                        //得到当前bucket是否有 UmxSpeedCompute 计算单元
                        UmxSpeedCompute speed = speeds.get(bucket);
                        if (speed == null) {
                            //把当前的时间和经纬度放进去
                            speed = new UmxSpeedCompute(values1.nextValue(), values2.nextValue());
                            //根据bucketid 存入objectarray
                            speeds.set(bucket, speed);
                        } else {
                            //若干存在计算单元，则将当前的时间和经纬度进行计算
                            speed.add(values1.nextValue(), values2.nextValue());
                        }
                    }
                }
            }
        };
    }


    @Override
    public ScoreMode scoreMode() {
        return valuesSources != null && valuesSources.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * 释放集合
     */
    @Override
    public void close() {
        Releasables.close(speeds);
    }
}
