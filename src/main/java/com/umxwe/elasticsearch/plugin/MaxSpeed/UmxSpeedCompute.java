package com.umxwe.elasticsearch.plugin.MaxSpeed;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.DistanceUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @ClassName UmxDistanceCompute
 * @Description Todo
 * @Author owen(umxwe)
 * @Date 2021/2/24
 */
public class UmxSpeedCompute implements Writeable, Cloneable {
    private final static Logger logger = LoggerFactory.getLogger(UmxSpeedCompute.class);

    private double maxSpeed = Double.NEGATIVE_INFINITY;

    /**
     * treemap 保持key（时间）有序，即按照时间排序
     */
    private TreeMap<Double, GeoPoint> timeStampAndLocation;

    /**
     * 文档数量，只做统计计算，会作为返回结果
     */
    protected long docCount = 0;


    public UmxSpeedCompute() {
        init();
    }

    public UmxSpeedCompute(Double timestamp, GeoPoint location) {
        logger.info(" constuctor function timeStamp:{},location:{}", timestamp, location.toString());
        this.init();
        this.add(timestamp, location);
    }

    /**
     * 初始化
     */
    private void init() {
        timeStampAndLocation = new TreeMap<>();
    }

    /**
     * @param in
     * @throws IOException
     */
    public UmxSpeedCompute(StreamInput in) throws IOException {
        this();
        docCount = (Long) in.readGenericValue();
        maxSpeed = (double) in.readGenericValue();
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
        /**
         * 仅供测试使用，速度取随机值
         */
//        double speed= new Random().nextDouble();
//        maxSpeed = Math.max(maxSpeed, speed);
//        logger.info("maxSpeed:{}",maxSpeed);

        for (Map.Entry<Double, GeoPoint> item : timeStampAndLocation.entrySet()
        ) {
            /**
             * 使用ARC计算距离，转换为km为单位
             */
            double distance = GeoDistance.ARC.calculate(
                    location.lat(), location.lon()
                    , item.getValue().lat(), item.getValue().lon()
                    , DistanceUnit.KILOMETERS);

            /**
             * 计算时间差，将时间转换为小时为单位
             */
            double timeInterval = Math.abs(item.getKey() - timestamp) / 1000 * 60 * 60;


            if (timeInterval == 0.0) {
                //时间差为0，直接返回负无穷大
                maxSpeed = Math.max(maxSpeed, Double.NEGATIVE_INFINITY);
                return;
            } else if (timeInterval == 0.0 && distance > 80) {
                //时间差为0，并且距离大于80km以上，直接返回正无穷大，说明存在套牌车
                maxSpeed = Math.max(maxSpeed, Double.POSITIVE_INFINITY);
                return;
            }

            /**
             * 计算当前速度
             */
            double currentSpeed = distance / timeInterval;
            logger.info("distance:{},timeInterval:{},currentSpeed:{}", distance, timeInterval, currentSpeed);

            /**
             * 计算速度最大值
             */
            maxSpeed = Math.max(maxSpeed, currentSpeed);
        }
        logger.info("speed_time:{} ms", System.currentTimeMillis() - current);
        timeStampAndLocation.put(timestamp, location);
    }

    /**
     * 多个计算单元进行合并，主要将最大速度和次数进行合并
     *
     * @param other
     */
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

    /**
     * 获取最大速度
     *
     * @return
     */
    public double getMaxSpeed() {
        return maxSpeed;
    }
}
