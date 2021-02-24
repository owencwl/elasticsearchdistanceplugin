package com.umxwe.elasticsearch.plugin.distance;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * @ClassName UmxDistanceCompute
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class UmxSpeedCompute implements Writeable, Cloneable{

    public UmxSpeedCompute() {
    }

    public UmxSpeedCompute(String[] fieldNames, double[] field1Vals, GeoPoint[] field2Vals) {
    }

    private void init(){

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public void add(String[] fieldNames, double[] field1Vals, GeoPoint[] field2Vals){

    }
    public void merge(final UmxSpeedCompute other ){

    }
}
