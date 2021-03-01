package com.umxwe.elasticsearch.plugin.MaxSpeed;

import com.umxwe.elasticsearch.plugin.MaxSpeed.support.ArrayValuesSourceAggregationBuilder;
import com.umxwe.elasticsearch.plugin.MaxSpeed.support.ArrayValuesSourceAggregatorFactory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
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
public class UmxMaxSpeedAggregationBuilder extends ArrayValuesSourceAggregationBuilder.LeafOnly<UmxMaxSpeedAggregationBuilder> {
    private final static Logger logger = LoggerFactory.getLogger(UmxMaxSpeedAggregationBuilder.class);

    public static final String NAME = "umxmaxspeed";

    public UmxMaxSpeedAggregationBuilder(String name) {
        super(name);
    }

    public UmxMaxSpeedAggregationBuilder(UmxMaxSpeedAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);

    }

    public UmxMaxSpeedAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        // Do nothing, no extra state to write to stream
    }

    @Override
    protected ArrayValuesSourceAggregatorFactory innerBuild(QueryShardContext queryShardContext, Map<String, ValuesSourceConfig> configs, AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
        return new UmxMaxSpeedAggregatorFactory(name, configs, queryShardContext, parent, subFactoriesBuilder, metadata);

    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new UmxMaxSpeedAggregationBuilder(this, factoriesBuilder, metadata);
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
