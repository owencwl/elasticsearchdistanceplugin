package com.umxwe.elasticsearch.plugin.MaxSpeed;

import com.umxwe.elasticsearch.plugin.MaxSpeed.support.ArrayValuesSourceAggregationBuilder;
import com.umxwe.elasticsearch.plugin.MaxSpeed.support.ArrayValuesSourceParser;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Map;

import static com.umxwe.elasticsearch.plugin.MaxSpeed.support.ArrayValuesSourceAggregationBuilder.MULTIVALUE_MODE_FIELD;

/**
 * @ClassName UmxDistanceParser
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class UmxMaxSpeedParser extends ArrayValuesSourceParser.NumericValuesSourceParser {

    public UmxMaxSpeedParser() {
        super(true);
    }

    @Override
    protected ArrayValuesSourceAggregationBuilder<?> createFactory(String aggregationName, ValuesSourceType valuesSourceType, ValueType targetValueType, Map<ParseField, Object> otherOptions) {
        UmxMaxSpeedAggregationBuilder builder = new UmxMaxSpeedAggregationBuilder(aggregationName);
        return builder;
    }

    @Override
    protected boolean token(String aggregationName, String currentFieldName, XContentParser.Token token, XContentParser parser, Map<ParseField, Object> otherOptions) throws IOException {
        if (MULTIVALUE_MODE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
            if (token == XContentParser.Token.VALUE_STRING) {
                otherOptions.put(MULTIVALUE_MODE_FIELD, parser.text());
                return true;
            }
        }
        return false;
    }
}
