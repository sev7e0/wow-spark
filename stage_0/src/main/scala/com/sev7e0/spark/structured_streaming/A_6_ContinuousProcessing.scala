package com.sev7e0.spark.structured_streaming

import org.apache.spark.sql.SparkSession

object A_6_ContinuousProcessing {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("ContinuousProcessing")
      .master("local")
      .getOrCreate()

    import org.apache.spark.sql.streaming.Trigger
    session.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.server", "host1:port1,host2:port2")
      .option("subscribe", "outPutTopic")
      .trigger(Trigger.Continuous("1 second"))
      .start()

    /**
      * 在spark2.3后,ContinuousProcessing(连续处理模式)目前仅支持一些查询操作
      * Operation:
      *   1.DF/DS仅支持projections(select, map, flatMap, mapPartitions, etc.) and selections (where, filter, etc.)
      *   2.支持所有的SQL操作(除了aggregation,current_timestamp(),current_date())
      * Source:
      *   1.kafka Source:全部支持
      *   2.Rate Source:适合测试。只有连续模式支持的选项是numPartitions和rowsPerSecond。
      * Sinks:
      *   1.Kafka Sink:全部支持
      *   2.Memory Sink:适合debugging
      *   2.Console Sink:适合debugging,控制台将会打印每个时间间隔的信息
      */
    /**
      * 注意事项:
      *   1.ContinuousProcessing模式下有多少个task取决于有多少个partition,他们将会并发进行读取,因此,在任务开始后要确保机器资源充足
      *   2.在停止ContinuousProcessing Stream时,task将会发出警告,可以忽略
      *   3.spark将会自动重试失败的task,失败的task数量过多将会导致query停止,需要从对应的checkpoint重新启动
      *
      */
  }

}
