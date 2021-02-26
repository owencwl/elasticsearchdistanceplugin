package com.umxwe.elasticsearch.plugin.distance;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.DistanceUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName UmxDistanceCompute
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class UmxSpeedCompute implements Writeable, Cloneable {

    private final static Logger logger = LoggerFactory.getLogger(UmxSpeedCompute.class);

    private double maxSpeed = Double.NEGATIVE_INFINITY;

    private HashMap<Double, GeoPoint> timeStampAndLocation;
    protected long docCount = 0;

    public UmxSpeedCompute() {
        init();
    }

    public UmxSpeedCompute(Double timestamp, GeoPoint location) {
        logger.info(" constuctor function timeStamp:{},location:{}", timestamp, location.toString());
        this.init();
        this.add(timestamp, location);
    }

    private void init() {
        timeStampAndLocation = new HashMap<>();
    }

    public UmxSpeedCompute(StreamInput in) throws IOException {
        docCount = (Long) in.readGenericValue();

    }

    // Convert Map to HashMap if it isn't
    private static <K, V> HashMap<K, V> convertIfNeeded(Map<K, V> map) {
        if (map instanceof HashMap) {
            return (HashMap<K, V>) map;
        } else {
            return new HashMap<>(map);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(docCount);
        out.writeGenericValue(maxSpeed);
    }

    /**
     * 最大速度计算逻辑，接受每一次数据的到来
     *
     * @param timestamp
     * @param location
     */
    public void add(Double timestamp, GeoPoint location) {
        logger.info("add function timeStamp:{},location:{}", timestamp, location.toString());
        if (timestamp == null) {
            throw new IllegalArgumentException("Cannot add statistics without field Double.");
        } else if (location == null) {
            throw new IllegalArgumentException("Cannot add statistics without field GeoPoint.");
        }
        ++docCount;
        long current = System.currentTimeMillis();

        if (timeStampAndLocation.size() == 0) {
            timeStampAndLocation.put(timestamp, location);
            maxSpeed = 0.0;
            return;
        }

        for (Map.Entry<Double, GeoPoint> item : timeStampAndLocation.entrySet()
        ) {
            double distance = GeoDistance.ARC.calculate(
                    location.lat(), location.lon()
                    , item.getValue().lat(), item.getValue().lon()
                    , DistanceUnit.KILOMETERS);

            double time = Math.abs(item.getKey() - timestamp) / 1000 * 60 * 60;

            if(time==0.0){
                //时间差为0，直接返回负无穷大
                maxSpeed = Math.max(maxSpeed, Double.NEGATIVE_INFINITY);
                return;
            }else if(time==0.0 && distance > 80){
                //时间差为0，并且距离大于80km以上，直接返回正无穷大，说明存在套牌车
                maxSpeed = Math.max(maxSpeed, Double.POSITIVE_INFINITY);
                return;
            }

            double speed = distance / time;
            logger.info("distance:{},time:{},speed:{}", distance, time, speed);
            maxSpeed = Math.max(maxSpeed, speed);
        }
        logger.info("distance_time:{} ms", System.currentTimeMillis() - current);
        timeStampAndLocation.put(timestamp, location);
    }

    public void merge(final UmxSpeedCompute other) {
        if (other == null) {
            return;
        } else if (this.docCount == 0) {
            this.maxSpeed = other.maxSpeed;
            this.docCount = other.docCount;
            return;
        }
        this.docCount += other.docCount;
        this.maxSpeed = Math.max(this.maxSpeed, other.maxSpeed);
    }

    public double getMaxSpeed() {
        return maxSpeed;
    }
}
