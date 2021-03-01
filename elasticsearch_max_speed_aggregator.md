### 场景介绍：

#### 套牌车分析

没有输入指定车牌，分析当前时间段内同号车身颜色不同  或  同号车辆类型不同  或  同号车辆品牌不同  或  同号短时异地出没  的车辆信息。（返回最多1000条结果）

`关键算法`：t1~t2 有很多分组的轨迹数据，按照车牌分组（keyby），对每个分组寻找不同的属性（颜色、类型、品牌、短时异地出没）

短时异地出没：对每个车牌分组分析，在所有记录中根据记录中的经纬度和抓拍时间计算速度，超过120km/h则是短时异地出没

![image-20210218092733914](file://G:\优美文档\um_work_log\2021.1.4worklog.assets\image-20210218092733914.png?lastModify=1614593633)

#### es自定义aggregator的步骤

`本插件基于es7.9.1上实现`

实现SearchPlugin类

```java
public class UmxMaxSpeedAggregationPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<SearchPlugin.AggregationSpec> getAggregations() {
        return singletonList(new SearchPlugin.AggregationSpec(UmxMaxSpeedAggregationBuilder.NAME, UmxMaxSpeedAggregationBuilder::new,
                new UmxMaxSpeedParser()).addResultReader(InternalUmxMaxSpeed::new));
    }
}
```



插件配置文件

plugin-descriptor.properties

```properties
description=maxspeed aggregation plugin
#
# 'version': plugin's version
version=7.9.1
#
# 'name': the plugin name
name=umxmaxspeed-aggregation-plugin
#
# 'classname': the name of the class to load, fully-qualified.
classname=${elasticsearch.plugin.classname}
#
# 'java.version': version of java the code is built against
# use the system property java.specification.version
# version string must be a sequence of nonnegative decimal integers
# separated by "."'s and may have leading zeros
java.version=${maven.compiler.target}
#
# 'elasticsearch.version': version of elasticsearch compiled against
elasticsearch.version=${elasticsearch.version}
```



maven依赖：

```xml
<properties>
    <fastjson>1.2.73</fastjson>
    <elasticsearch.version>7.9.1</elasticsearch.version>
    <elasticsearch.assembly.descriptor>${project.basedir}/src/main/assemblies/plugin.xml
    </elasticsearch.assembly.descriptor>
    <elasticsearch.plugin.name>elasticsearch-distance-plugin</elasticsearch.plugin.name>
    <elasticsearch.plugin.classname>com.umxwe.elasticsearch.plugin.MaxSpeed.UmxMaxSpeedAggregationPlugin
    </elasticsearch.plugin.classname>
    <elasticsearch.plugin.jvm>true</elasticsearch.plugin.jvm>
    <tests.rest.load_packaged>false</tests.rest.load_packaged>
    <skip.unit.tests>true</skip.unit.tests>
    <maven.compiler.target>1.8</maven.compiler.target>
</properties>
<dependencies>
    <dependency>
        <groupId>org.elasticsearch</groupId>
        <artifactId>elasticsearch</artifactId>
        <version>${elasticsearch.version}</version>
        <scope>provided</scope>
    </dependency>
    <!-- https://mvnrepository.com/artifact/org.roaringbitmap/RoaringBitmap -->
    <dependency>
        <groupId>org.roaringbitmap</groupId>
        <artifactId>RoaringBitmap</artifactId>
        <version>0.9.0</version>
    </dependency>
    <!--log4j2 start-->
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-api</artifactId>
        <version>2.11.1</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-core</artifactId>
        <version>2.11.1</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-1.2-api</artifactId>
        <version>2.11.1</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.logging.log4j</groupId>
        <artifactId>log4j-slf4j-impl</artifactId>
        <version>2.11.1</version>
    </dependency>
    <dependency>
        <groupId>com.lmax</groupId>
        <artifactId>disruptor</artifactId>
        <version>3.3.2</version>
    </dependency>
    <!--log4j2 end-->
    <dependency>
        <groupId>com.alibaba</groupId>
        <artifactId>fastjson</artifactId>
        <version>${fastjson}</version>
    </dependency>
</dependencies>
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-surefire-plugin</artifactId>
            <version>2.11</version>
            <configuration>
                <includes>
                    <include>**/*Tests.java</include>
                </includes>
            </configuration>
        </plugin>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-javadoc-plugin</artifactId>
            <version>2.9</version>
        </plugin>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-source-plugin</artifactId>
            <version>2.2.1</version>
        </plugin>
        <plugin>
            <artifactId>maven-assembly-plugin</artifactId>
            <version>2.3</version>
            <configuration>
                <appendAssemblyId>false</appendAssemblyId>
                <outputDirectory>${project.build.directory}/releases/</outputDirectory>
                <descriptors>
                    <descriptor>${basedir}/src/main/assemblies/plugin.xml</descriptor>
                </descriptors>
            </configuration>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>single</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <configuration>
                <source>8</source>
                <target>8</target>
            </configuration>
        </plugin>
    </plugins>
</build>
```





短时异地出没关键逻辑：

- 使用TreeMap存储每个bucket的数据
- 对每一个到来的数据循环计算速度
- 速度计算方法：两个geo_point的经纬度，计算出距离，单位为km;然后计算时间差，转换为小时，最后距离除以时间得到速度（km/h）

```java
private TreeMap<Double, GeoPoint> timeStampAndLocation;

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
```



打包方式：

mvn package

```shell
[WARNING] File encoding has not been set, using platform encoding GBK, i.e. build is platform dependent!
[INFO] Building zip: G:\github\elasticsearchdistanceplugin\target\releases\elasticsearchdistanceplugin-1.0-SNAPSHOT.zip
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  2.712 s
[INFO] Finished at: 2021-03-01T17:29:23+08:00
[INFO] ------------------------------------------------------------------------

G:\github\elasticsearchdistanceplugin>mvn package
```

打开kibana进入到dev_tool页面，

http://10.116.200.24:5601/app/dev_tools#/console

```json
//创建索引
PUT  vehicletrajectoryentitytemp
//创建mapping映射
POST vehicletrajectoryentitytemp/_mapping?pretty
{
  "properties":{
    "shotTime":{
      "type": "long"
    },
    "location":{
      "type":"geo_point"
    } ,
    "plateNo": { 
      "type": "keyword"
    }
  }
}

//插入数据
POST vehicletrajectoryentitytemp/_doc
{
  "shotTime":1611242548346,
  "location":"32.136553,131.172638",
  "plateNo":"湘YUTQU3"
}

//验证自定义聚合器
POST vehicletrajectoryentitytemp/_search
{
  
    "aggs": {
        "speed": {
          "umxmaxspeed": {
            "fields": [
              "shotTime",
              "location"
            ]
          }
        }
      }
}
```



发送复杂groupby+umxmaxspeed聚合查询请求：

```json
POST carindex/_search
{

  "aggregations": {
    "GroupbyPlateNo": {
      "terms": {
        "field": "plateNo",
        "size": 10,
        "min_doc_count": 1,
        "shard_min_doc_count": 0,
        "show_term_doc_count_error": false,
        "order": [
          {
            "_count": "desc"
          },
          {
            "_key": "asc"
          }
        ]
      },
      "aggs": {
        "speed": {
          "umxmaxspeed": {
            "fields": [
              "shotTime",
              "location"
            ]
          }
        }
      }
    }
  }
}
```

es聚合查询结果：

```json
{
  "took" : 1199,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 10000,
      "relation" : "gte"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "iptO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇90",
          "deviceID" : "4312000000119000090",
          "deviceName" : "摄像头设备90",
          "location" : "28.323137,113.129291",
          "plateClass" : "embassy license plate",
          "plateClassDesc" : "港澳入出境牌",
          "plateColor" : "Brown",
          "plateColorDesc" : "棕",
          "plateNo" : "台PYIWYI",
          "rowKey" : "220210127PYIWYI2d190",
          "shotPlaceLatitude" : 28.323137,
          "shotPlaceLongitude" : 113.129291,
          "shotTime" : 1611735864672,
          "vehicleBrand" : "white",
          "vehicleBrandDesc" : "福汽启腾",
          "vehicleClass" : "Heavy duty full trailer",
          "vehicleClassDesc" : "微型车",
          "vehicleColor" : "blue",
          "vehicleColorDesc" : "红色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "jZtO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇99",
          "deviceID" : "4312000000119000099",
          "deviceName" : "摄像头设备99",
          "location" : "28.235824,112.921953",
          "plateClass" : "armed police license plate",
          "plateClassDesc" : "武警车牌",
          "plateColor" : "Brown",
          "plateColorDesc" : "灰",
          "plateNo" : "桂IITIQO",
          "rowKey" : "320210101IITIQO81890",
          "shotPlaceLatitude" : 28.235824,
          "shotPlaceLongitude" : 112.921953,
          "shotTime" : 1609498422433,
          "vehicleBrand" : "Mazda",
          "vehicleBrandDesc" : "奇瑞",
          "vehicleClass" : "advanced car",
          "vehicleClassDesc" : "微型车",
          "vehicleColor" : "Brown",
          "vehicleColorDesc" : "红色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "j5tO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇82",
          "deviceID" : "4312000000119000082",
          "deviceName" : "摄像头设备82",
          "location" : "28.167558,113.08314",
          "plateClass" : "Hong Kong Macao Entry exit license plate",
          "plateClassDesc" : "武警车牌",
          "plateColor" : "green",
          "plateColorDesc" : "黑 ",
          "plateNo" : "桂ZUIEPP",
          "rowKey" : "320210103ZUIEPP2df20",
          "shotPlaceLatitude" : 28.167558,
          "shotPlaceLongitude" : 113.08314,
          "shotTime" : 1609606793655,
          "vehicleBrand" : "Futian",
          "vehicleBrandDesc" : "特斯拉",
          "vehicleClass" : "Heavy duty full trailer",
          "vehicleClassDesc" : "小型车",
          "vehicleColor" : "blue",
          "vehicleColorDesc" : "白色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "mptO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇85",
          "deviceID" : "4312000000119000085",
          "deviceName" : "摄像头设备85",
          "location" : "28.231591,113.18182",
          "plateClass" : "armed police license plate",
          "plateClassDesc" : "教练车牌",
          "plateColor" : "white",
          "plateColorDesc" : "黄",
          "plateNo" : "陕JOORRU",
          "rowKey" : "320210101JOORRU79641",
          "shotPlaceLatitude" : 28.231591,
          "shotPlaceLongitude" : 113.18182,
          "shotTime" : 1609468748375,
          "vehicleBrand" : "Huanghai",
          "vehicleBrandDesc" : "法拉利",
          "vehicleClass" : "advanced car",
          "vehicleClassDesc" : "微型车",
          "vehicleColor" : "Silver gray",
          "vehicleColorDesc" : "棕色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "nptO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇30",
          "deviceID" : "4312000000119000030",
          "deviceName" : "摄像头设备30",
          "location" : "28.320217,113.149249",
          "plateClass" : "embassy license plate",
          "plateClassDesc" : "军车车牌",
          "plateColor" : "yellow",
          "plateColorDesc" : "灰",
          "plateNo" : "浙CQRYTI",
          "rowKey" : "320210117CQRYTI083da",
          "shotPlaceLatitude" : 28.320217,
          "shotPlaceLongitude" : 113.149249,
          "shotTime" : 1610822794073,
          "vehicleBrand" : "BAIC Weiwang",
          "vehicleBrandDesc" : "奔腾",
          "vehicleClass" : "Hatchback",
          "vehicleClassDesc" : "MPV车型",
          "vehicleColor" : "Brown",
          "vehicleColorDesc" : "红色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "pJtO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇98",
          "deviceID" : "4312000000119000098",
          "deviceName" : "摄像头设备98",
          "location" : "28.16053,113.011709",
          "plateClass" : "coach license plate",
          "plateClassDesc" : "民用车牌",
          "plateColor" : "gray",
          "plateColorDesc" : "棕",
          "plateNo" : "晋BITTRR",
          "rowKey" : "020210101BITTRRccfe2",
          "shotPlaceLatitude" : 28.16053,
          "shotPlaceLongitude" : 113.011709,
          "shotTime" : 1609477879994,
          "vehicleBrand" : "Isuzu",
          "vehicleBrandDesc" : "宇通",
          "vehicleClass" : "Heavy duty full trailer",
          "vehicleClassDesc" : "高级车型",
          "vehicleColor" : "black",
          "vehicleColorDesc" : "红色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "qZtO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇87",
          "deviceID" : "4312000000119000087",
          "deviceName" : "摄像头设备87",
          "location" : "28.15088,112.941422",
          "plateClass" : "embassy license plate",
          "plateClassDesc" : "国外车牌",
          "plateColor" : "Brown",
          "plateColorDesc" : "红",
          "plateNo" : "粤WQORWR",
          "rowKey" : "120210130WQORWRd8fc9",
          "shotPlaceLatitude" : 28.15088,
          "shotPlaceLongitude" : 112.941422,
          "shotTime" : 1612012092455,
          "vehicleBrand" : "Baojun",
          "vehicleBrandDesc" : "卡威",
          "vehicleClass" : "medium car",
          "vehicleClassDesc" : "高级车型",
          "vehicleColor" : "white",
          "vehicleColorDesc" : "蓝色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "rZtO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇40",
          "deviceID" : "4312000000119000040",
          "deviceName" : "摄像头设备40",
          "location" : "28.209786,112.929684",
          "plateClass" : "foreign license plate",
          "plateClassDesc" : "挂车号牌",
          "plateColor" : "Red",
          "plateColorDesc" : "灰",
          "plateNo" : "鄂KUQEEU",
          "rowKey" : "020210101KUQEEUa9a65",
          "shotPlaceLatitude" : 28.209786,
          "shotPlaceLongitude" : 112.929684,
          "shotTime" : 1609464616787,
          "vehicleBrand" : "Hengtian automobile",
          "vehicleBrandDesc" : "宇通",
          "vehicleClass" : "Heavy duty full trailer",
          "vehicleClassDesc" : "CDV车型",
          "vehicleColor" : "white",
          "vehicleColorDesc" : "银灰色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "r5tO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇23",
          "deviceID" : "4312000000119000023",
          "deviceName" : "摄像头设备23",
          "location" : "28.093414,112.916708",
          "plateClass" : "coach license plate",
          "plateClassDesc" : "挂车号牌",
          "plateColor" : "Red",
          "plateColorDesc" : "绿",
          "plateNo" : "贵ZQPEYT",
          "rowKey" : "320210106ZQPEYT6ca8d",
          "shotPlaceLatitude" : 28.093414,
          "shotPlaceLongitude" : 112.916708,
          "shotTime" : 1609935193349,
          "vehicleBrand" : "Benz",
          "vehicleBrandDesc" : "九龙",
          "vehicleClass" : "MPV",
          "vehicleClassDesc" : "小型车",
          "vehicleColor" : "black",
          "vehicleColorDesc" : "白色"
        }
      },
      {
        "_index" : "carindex",
        "_type" : "carindex",
        "_id" : "t5tO7HcBlDjRggDJFvyI",
        "_score" : 1.0,
        "_source" : {
          "address" : "湖南省怀化市溆浦县卢峰镇7",
          "deviceID" : "431200000011900007",
          "deviceName" : "摄像头设备7",
          "location" : "28.288339,112.875006",
          "plateClass" : "foreign license plate",
          "plateClassDesc" : "民用车牌",
          "plateColor" : "Brown",
          "plateColorDesc" : "黑 ",
          "plateNo" : "贵AUTPYQ",
          "rowKey" : "220210112AUTPYQ74912",
          "shotPlaceLatitude" : 28.288339,
          "shotPlaceLongitude" : 112.875006,
          "shotTime" : 1610412356229,
          "vehicleBrand" : "Jiulong",
          "vehicleBrandDesc" : "GMC",
          "vehicleClass" : "SUV",
          "vehicleClassDesc" : "三厢车型",
          "vehicleColor" : "black",
          "vehicleColorDesc" : "黄色"
        }
      }
    ]
  },
  "aggregations" : {
    "GroupbyPlateNo" : {
      "doc_count_error_upper_bound" : 10,
      "sum_other_doc_count" : 1144780,
      "buckets" : [
        {
          "key" : "桂WRTIOE",
          "doc_count" : 3,
          "speed" : {
            "doc_count" : 3,
            "MaxSpeed" : 3.092015692061709E-9
          }
        },
        {
          "key" : "云AOEROO",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 2.434445258142108E-8
          }
        },
        {
          "key" : "云AQWWWY",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 1.8750692701540827E-9
          }
        },
        {
          "key" : "云ATYTEE",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 0.0
          }
        },
        {
          "key" : "云BWIUPQ",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 7.278775319977958E-9
          }
        },
        {
          "key" : "云DPRWTI",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 5.29249044460409E-9
          }
        },
        {
          "key" : "云DUEPUQ",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 5.62642091226822E-10
          }
        },
        {
          "key" : "云EWUQIW",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 0.0
          }
        },
        {
          "key" : "云FPOEYI",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 1.436623586842334E-9
          }
        },
        {
          "key" : "云FTPRYO",
          "doc_count" : 2,
          "speed" : {
            "doc_count" : 2,
            "MaxSpeed" : 2.4492226857428478E-8
          }
        }
      ]
    }
  }
}

```

