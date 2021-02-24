package com.umxwe.elasticsearch.plugin.distance.support;

/**
 * @ClassName ArrayValuesSourceAggregatorFactory
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class ArrayValuesSourceAggregatorFactory
        extends AggregatorFactory {

    protected Map<String, ValuesSourceConfig> configs;

    public ArrayValuesSourceAggregatorFactory(String name, Map<String, ValuesSourceConfig> configs,
                                              QueryShardContext queryShardContext, AggregatorFactory parent,
                                              AggregatorFactories.Builder subFactoriesBuilder,
                                              Map<String, Object> metadata) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.configs = configs;
    }

    @Override
    public Aggregator createInternal(SearchContext searchContext,
                                     Aggregator parent,
                                     CardinalityUpperBound cardinality,
                                     Map<String, Object> metadata) throws IOException {
        HashMap<String, ValuesSource> valuesSources = new HashMap<>();

        for (Map.Entry<String, ValuesSourceConfig> config : configs.entrySet()) {
            ValuesSourceConfig vsc = config.getValue();
            if (vsc.hasValues()) {
                valuesSources.put(config.getKey(), vsc.getValuesSource());
            }
        }
        if (valuesSources.isEmpty()) {
            return createUnmapped(searchContext, parent, metadata);
        }
        return doCreateInternal(valuesSources, searchContext, parent, cardinality, metadata);
    }

    /**
     * Create the {@linkplain Aggregator} when none of the configured
     * fields can be resolved to a {@link ValuesSource}.
     */
    protected abstract Aggregator createUnmapped(SearchContext searchContext,
                                                 Aggregator parent,
                                                 Map<String, Object> metadata) throws IOException;

    /**
     * Create the {@linkplain Aggregator} when any of the configured
     * fields can be resolved to a {@link ValuesSource}.
     *
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that the {@link Aggregator} created by this method
     *                    will be asked to collect.
     */
    protected abstract Aggregator doCreateInternal(Map<String, ValuesSource> valuesSources,
                                                   SearchContext searchContext,
                                                   Aggregator parent,
                                                   CardinalityUpperBound cardinality,
                                                   Map<String, Object> metadata) throws IOException;

}