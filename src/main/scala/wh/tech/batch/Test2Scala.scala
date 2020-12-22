package wh.tech.batch

import org.apache.flink.api.java.utils.MultipleParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Test2Scala {
  private val PATH = "/flink/user_behavior"
  def main(args: Array[String]): Unit = {
    val params = MultipleParameterTool.fromArgs(args)

    // val env = StreamExecutionEnvironment.createLocalEnvironment()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)

    val tab_env = StreamTableEnvironment.create(env)
    val topic="user_behavior"
    val servers="52.81.252.67:9092"
    val source_ddl = s""" CREATE TABLE kafka_behavior(
                       |        user_id BIGINT,
                       |        item_id BIGINT,
                       |        category_id BIGINT,
                       |        behavior STRING,
                       |        ts TIMESTAMP(3)
                       |      ) WITH (
                       |    'connector' = 'kafka',
                       |    'topic' = '$topic',
                       |    'properties.bootstrap.servers' = '$servers',
                       |    'format' = 'json',
                       |    'json.fail-on-missing-field' = 'false',
                       |    'json.ignore-parse-errors' = 'true'
                       |    )""".stripMargin

    tab_env.executeSql(source_ddl)

    val path =
      if (params.has("path"))
        params.get("path")
      else
        PATH
    val target_ddl = s"""  CREATE TABLE s3_behavior(
                       |       user_id BIGINT,
                       |       item_id BIGINT,
                       |       category_id BIGINT,
                       |       behavior VARCHAR,
                       |       ts TIMESTAMP(3)
                       |   ) WITH (
                       |     'connector' = 'filesystem',
                       |     'path' = '$path',
                       |     'format' = 'parquet'
                       |   )
                       |   """.stripMargin

    tab_env.executeSql(target_ddl)

    tab_env.executeSql("insert into s3_behavior select * from kafka_behavior")
    //tab_env.execute("scala test")
  }
}
