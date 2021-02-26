package com.umxwe.elasticsearch.plugin.distance;

import com.alibaba.fastjson.JSON;
import com.umxwe.elasticsearch.plugin.distance.support.ArrayValuesSourceAggregatorFactory;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName UmxDistanceAggregatorFactory
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class UmxDistanceAggregatorFactory extends ArrayValuesSourceAggregatorFactory {
    private final static Logger logger = LoggerFactory.getLogger(UmxDistanceAggregatorFactory.class);

    public UmxDistanceAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs, QueryShardContext queryShardContext, AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metadata) throws IOException {
        super(name, configs, queryShardContext, parent, subFactoriesBuilder, metadata);

    }

    @Override
    protected Aggregator doCreateInternal(Map<String, ValuesSource> valuesSources, SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {
//        Map<String, ValuesSource.Numeric> typedValuesSources = new HashMap<>(valuesSources.size());
//        for (Map.Entry<String, ValuesSource> entry : valuesSources.entrySet()) {
//
//            if (entry.getValue() instanceof ValuesSource.Numeric == false) {
//                throw new AggregationExecutionException("ValuesSource type " + entry.getValue().toString() +
//                        "is not supported for aggregation " + this.name());
//            }
//            // TODO: There must be a better option than this.
//            typedValuesSources.put(entry.getKey(), (ValuesSource.Numeric) entry.getValue());
//        }
        logger.info("doCreateInternal-valuesSources-size:{}", JSON.toJSONString(valuesSources.size()));
        return new UmxDistanceAggregator(name, valuesSources, searchContext, parent,  metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new UmxDistanceAggregator(name, null, searchContext, parent, metadata);
    }

}
