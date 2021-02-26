package com.umxwe.elasticsearch.plugin.distance;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

/**
 * @ClassName UmxDistanceAggregator
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class UmxDistanceAggregator extends MetricsAggregator {
    private final static Logger logger = LoggerFactory.getLogger(UmxDistanceAggregator.class);

    private final ArrayValuesSource.MultArrayValuesSource valuesSources;

    ObjectArray<UmxSpeedCompute> speeds;


    public UmxDistanceAggregator(String name, Map<String, ValuesSource> valuesSources, SearchContext searchContext, Aggregator aggregator, MultiValueMode multiValueMode, Map<String, Object> stringObjectMap) throws IOException {
        super(name, searchContext, aggregator, stringObjectMap);
        if (valuesSources != null && !valuesSources.isEmpty()) {
            this.valuesSources = new ArrayValuesSource.MultArrayValuesSource(valuesSources, multiValueMode);
            logger.info("UmxDistanceAggregator_values1_size:{},values2_size:{}"
                    , this.valuesSources.values1 != null ? this.valuesSources.values1 : 0
                    , this.valuesSources.values2 != null ? this.valuesSources.values2 : 0);

            speeds  = context.bigArrays().newObjectArray(1);

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
        return new InternalUmxDistance(name, 0,0, speed, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalUmxDistance(name, 0,0, null, metadata());
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
        final String[] fieldNames = valuesSources.fieldNames();
        //map<timestamp,geopoint>
        final Map<Double, GeoPoint> map = new HashMap<>();
        final List<Tuple<Double, GeoPoint>> list= new ArrayList<>();

        final BigArrays bigArrays = context.bigArrays();

        SortedNumericDoubleValues values1 = valuesSources.getValues1().doubleValues(ctx);
        MultiGeoPointValues values2 = valuesSources.getValues2().geoPointValues(ctx);

        logger.info("fieldNames:{},number_docValueCount:{},geo_docValueCount:{}", valuesSources.fieldNames(), values1.docValueCount(), values2.docValueCount());

//        Tuple values= new Tuple<>(values1,values2);
//        GeoDistance.ARC.calculate();
        //valuesSources 转为 number and  geopoint类型
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                logger.info("doc:{},bucket:{}", doc, bucket);
                // get fields
                if (values1.advanceExact(doc) && values2.advanceExact(doc)) {

                    /**
                     * 1、一般情况map size 等于 docValueCount
                     * 2、特殊情况，map size 小于 docValueCount ，说明时间戳存在重复的情况，说明在相同时间存在不同的坐标，直接判定为套牌车
                     */
//                    if (map.size() < values1.docValueCount()) {
//                        logger.info("时间戳存在重复的情况，说明在相同时间存在不同的坐标，直接判定为套牌车");
//                    }

//                    SortedNumericDoubleValues speedValues = computeSpeed(map,list);
                    speeds = bigArrays.grow(speeds, bucket + 1);

                    for (int i = 0; i < values1.docValueCount(); i++) {

                        UmxSpeedCompute speed = speeds.get(bucket);
                        if (speed == null) {
                            speed = new UmxSpeedCompute(values1.nextValue(), values2.nextValue());
                            speeds.set(bucket, speed);
                        } else {
                            speed.add(values1.nextValue(), values2.nextValue());
                        }

                    }
                }
            }

            /**
             * 速度计算逻辑
             * @param mapObject
             * @param list
             * @return
             */
            private SortedNumericDoubleValues computeSpeed(Map<Double, GeoPoint> mapObject,List<Tuple<Double, GeoPoint>> list) {

                logger.info("map_size:{},list_size:{}",mapObject.size(),list.size());

                /**
                 * 时间复杂度有点高，有待优化
                 */
               long current= System.currentTimeMillis();
                for (int i = 0; i <list.size() ; i++) {
                    for (int j = i+1; j <list.size() ; j++) {
                        double distance=  GeoDistance.ARC.calculate(
                                list.get(i).v2().lat(),list.get(i).v2().lon()
                                ,list.get(j).v2().lat(),list.get(j).v2().lon()
                                , DistanceUnit.KILOMETERS);
                        double time=Math.abs( list.get(i).v1()- list.get(j).v1())/1000*60*60;

                        double speed=distance/time;
                        logger.info("distance:{},time:{},speed:{}",distance,time,speed);
                        if(speed>80){
                            break;
                        }
                    }
                }
                logger.info("distance_time:{} ms",System.currentTimeMillis()-current);

                /**
                 * speed list need transform to SortedNumericDoubleValues
                 */

                SortedNumericDoubleValues mult = new SortedNumericDoubleValues() {

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return false;
                    }

                    @Override
                    public double nextValue() throws IOException {
                        double value = new Random().nextDouble();
                        logger.info("SortedNumericDoubleValues:{}", value);
                        return value;
                    }

                    @Override
                    public int docValueCount() {
                        return 10;
                    }
                };

                return mult;

            }

            /**
             * return a map of field names and data
             */
            private boolean includeDocument(int doc) throws IOException {

                if (values1.advanceExact(doc) && values2.advanceExact(doc)) {
                    final int valuesCount1 = values1.docValueCount();
                    final int valuesCount2 = values2.docValueCount();
                    if (valuesCount1 != valuesCount2) {
                        return false;
                    }
                    for (int i = 0; i < valuesCount1; i++) {
                        double timeStamp = values1.nextValue();
                        GeoPoint location = values2.nextValue();
                        logger.info("timeStamp:{},location:{}", timeStamp, location.toString());
                        map.put(timeStamp, location);
                        list.add(new Tuple<Double, GeoPoint>(timeStamp,location));
                        
                    }
                } else {
                    return false;
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
