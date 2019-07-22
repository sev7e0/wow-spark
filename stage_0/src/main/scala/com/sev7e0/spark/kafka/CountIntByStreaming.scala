package com.sev7e0.spark.kafka

import com.sev7e0.spark.spark_streaming.StreamingLogger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
;

object CountIntByStreaming {

  val brokerList = "localhost:9092"
  val topic = "randomCount"
  val groupId = "group";
  val path = "temp/checkpoint/CountIntBySS";
  val master = "local";

  def main(args: Array[String]): Unit = {
    val prop = initProperties()
    val topics = Array(topic)

    //设置打印日志级别
    StreamingLogger.setLoggerLevel()

    val sparkConf = new SparkConf()
      .setAppName(CountIntByStreaming.getClass.getName)
      .setMaster(master)

    //实例化StreamingContext，设置间隔两秒
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //设置checkpoint路径
    ssc.checkpoint(path)


    //使用KafkaUtils获取DStream
    val kafkaDS = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, prop))


    kafkaDS.map(record => {
      val value = record.value().toLong
      value
    }).reduce(_ + _).print()

    /**
      * 输出结果：
      *
      * -------------------------------------------
      * Time: 1561023450000 ms
      * -------------------------------------------
      * 7
      *
      * -------------------------------------------
      * Time: 1561023452000 ms
      * -------------------------------------------
      * 15
      *
      * -------------------------------------------
      * Time: 1561023454000 ms
      * -------------------------------------------
      * 11
      */
    ssc.start()

    ssc.awaitTermination()
  }

  /**
    * 初始化kafka客户端配置
    *
    * @return
    */
  def initProperties(): Map[String, Object] = Map[String, Object](
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    ConsumerConfig.GROUP_ID_CONFIG -> groupId,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
  )
}
