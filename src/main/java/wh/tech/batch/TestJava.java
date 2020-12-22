package wh.tech.batch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.log4j.Logger;
import wh.tech.batch.bean.UserBehavior;

import java.util.Date;
import java.util.Properties;


public class TestJava {

    private final static Logger logger = Logger.getLogger(TestJava.class);
    //private static final String S3_PATH = "s3://chengang-test/flink/kafka_user/";
    private static final String HDFS_PATH = "hdfs://ip-10-0-35-70.cn-north-1.compute.internal:8020/flink/kafka_user/";


    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);


        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        logger.info("设置 Kafka 连接配置");
        //获取文件中的内容
        Properties properties = new Properties();
        //这里是由一个kafka
//        properties.setProperty("bootstrap.servers", "10.0.35.187:9094");
        properties.setProperty("bootstrap.servers", "52.81.252.67:9092");
        //properties.setProperty("group.id", "test");

        logger.info("初始化 Kafka consumer");
        //第一个参数是topic的名称
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("user_behavior",
                new SimpleStringSchema(), properties);

        consumer.assignTimestampsAndWatermarks(
                (WatermarkStrategy<String>) context -> new WatermarkGenerator<String>(){
            // 每来一条数据，将这条数据与maxTimesStamp比较，看是否需要更新watermark
            private long maxTimesStamp = Long.MIN_VALUE;
            @Override
            public void onEvent(String event, long eventTimestamp, WatermarkOutput watermarkOutput) {
                long current = new Date().getTime();
                maxTimesStamp = Math.max(current, maxTimesStamp);
            }

            // 周期性更新watermark
            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                // 允许乱序数据的最大限度为3s
                long maxOutOfOrderness = 3000L;
                watermarkOutput.emitWatermark(new Watermark(maxTimesStamp - maxOutOfOrderness));
            }
        });


        DataStream<UserBehavior> stream = env.addSource(consumer).map(TestJava::transformData);
//        DataStream<UserBehavior> stream = env.addSource(consumer)
//            .timeWindowAll(Time.seconds(5)).process(
//            new ProcessAllWindowFunction<String, UserBehavior, TimeWindow>() {
//                @Override
//                public void process(Context context, Iterable<String> input, Collector<UserBehavior> output) throws Exception {
//                    logger.info("Process All WindowFunction");
//                    input.forEach(
//                        jsonStr -> {
//                            output.collect(transformData(jsonStr));
//                        }
//                    );
//                }
//            }
//        );

        // Send hdfs by parquet
        String path = HDFS_PATH;
        if(params.has("path")){
            path=params.get("path");
        }
        StreamingFileSink<UserBehavior> streamingFileSink = StreamingFileSink
                    .forBulkFormat(
                            new Path(path),
                            ParquetAvroWriters.forReflectRecord(UserBehavior.class)
                    ).withRollingPolicy(
                        OnCheckpointRollingPolicy.build()
                    ).build();

        logger.info("Parquet write to s3");
        stream.addSink(streamingFileSink).name("Sink To S3");

        stream.print();
        env.execute();
    }

    private static UserBehavior transformData(String data) {
        if (data != null && !data.isEmpty()) {
            JSONObject value = JSON.parseObject(data);
            UserBehavior userBehavior = new UserBehavior();
            userBehavior.setUserId(value.getString("user_id"));
            userBehavior.setItemId(value.getString("item_id"));
            userBehavior.setCategoryId(value.getString("category_id"));
            userBehavior.setBehavior(value.getString("behavior"));
            return userBehavior;
        } else {
            return new UserBehavior();
        }
    }
}

