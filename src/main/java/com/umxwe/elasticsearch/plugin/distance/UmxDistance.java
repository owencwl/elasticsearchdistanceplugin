package com.umxwe.elasticsearch.plugin.distance;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

public interface UmxDistance  extends Aggregation {

    /** return the total document count */
    long getDocCount();
    /** return the max speed */
    double getMaxSpeed();
}
