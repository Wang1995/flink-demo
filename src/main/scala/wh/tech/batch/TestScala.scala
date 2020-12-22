package wh.tech.batch

import java.util.{Date, Properties}

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.MultipleParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import wh.tech.batch.bean.UserBehavior


object TestScala {
  private val HDFS_PATH = "/flink/kafka_user/"

  def main(args: Array[String]): Unit = {
    val params = MultipleParameterTool.fromArgs(args)
//    val env = StreamExecutionEnvironment.createLocalEnvironment()
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)

    var prop = new Properties()
    //这里是由一个kafka
    prop.setProperty("bootstrap.servers", "52.81.252.67:9092")

    //第一个参数是topic的名称
    val consumer = new FlinkKafkaConsumer[String]("user_behavior", new SimpleStringSchema(), prop)

    val strategy = new WatermarkStrategy[String](){
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[String] = {
        new WatermarkGenerator[String] {
          private  var maxTimesStamp : Long = Long.MinValue
          override def onEvent(t: String, l: Long, watermarkOutput: WatermarkOutput): Unit =
            maxTimesStamp=Math.max(new Date().getTime, maxTimesStamp)
          override def onPeriodicEmit(watermarkOutput: WatermarkOutput): Unit = {
            val maxOutOfOrderness : Long= 3000L
            watermarkOutput.emitWatermark(new Watermark(maxTimesStamp - maxOutOfOrderness))
          }
        }
      }
    }

    consumer.assignTimestampsAndWatermarks(strategy)

    val stream : DataStream[UserBehavior] = env.addSource(consumer)
      .map(
        new MapFunction[String, UserBehavior] {
          override def map(t: String): UserBehavior = transformData(t)
        })

    val path =
      if (params.has("path"))
        params.get("path")
      else
        HDFS_PATH

    val sink : StreamingFileSink[UserBehavior] =
      StreamingFileSink.forBulkFormat(
        new Path(path),
        ParquetAvroWriters.forReflectRecord(classOf[UserBehavior])
      ).withRollingPolicy(
        OnCheckpointRollingPolicy.build()
      ).build

    stream.addSink(sink)
    env.execute()
  }

  private def transformData(data: String) =
    if (data != null && !data.isEmpty) {
      val value = JSON.parseObject(data)
      val userBehavior = new UserBehavior
      userBehavior.setUserId(value.getString("user_id"))
      userBehavior.setItemId(value.getString("item_id"))
      userBehavior.setCategoryId(value.getString("category_id"))
      userBehavior.setBehavior(value.getString("behavior"))
      userBehavior
    }
    else
      new UserBehavior
}
