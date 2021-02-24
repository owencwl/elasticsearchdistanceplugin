package com.umxwe.elasticsearch.plugin.distance;

import com.umxwe.elasticsearch.plugin.distance.support.ArrayValuesSourceAggregationBuilder;
import com.umxwe.elasticsearch.plugin.distance.support.ArrayValuesSourceAggregatorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName UmxDistanceAggregationBuilder
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/23
 */
public class UmxDistanceAggregationBuilder extends ArrayValuesSourceAggregationBuilder.LeafOnly<UmxDistanceAggregationBuilder> {
    private final static Logger logger = LoggerFactory.getLogger(UmxDistanceAggregationBuilder.class);

    public static final String NAME = "umxdistance";
    private MultiValueMode multiValueMode = MultiValueMode.AVG;

    public UmxDistanceAggregationBuilder(String name) {
        super(name);
    }

    public UmxDistanceAggregationBuilder(UmxDistanceAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.multiValueMode = clone.multiValueMode;

    }

    public UmxDistanceAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        // Do nothing, no extra state to write to stream
    }
    public UmxDistanceAggregationBuilder multiValueMode(MultiValueMode multiValueMode) {
        this.multiValueMode = multiValueMode;
        return this;
    }
    public MultiValueMode multiValueMode() {
        return this.multiValueMode;
    }

    @Override
    protected ArrayValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext, Map<String, ValuesSourceConfig> configs, AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new UmxDistanceAggregatorFactory(name, multiValueMode, configs, queryShardContext, parent, subFactoriesBuilder, metadata);

    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new UmxDistanceAggregationBuilder(this, factoriesBuilder, metadata);
    }


    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
}
