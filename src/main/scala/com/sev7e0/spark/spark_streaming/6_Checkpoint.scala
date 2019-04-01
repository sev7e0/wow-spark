package com.sev7e0.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @program: spark-learn
  * @description: 流式应用可能是7*24小时运行的，因此要保证适应与应用程序逻辑无关的故障（jvm 、操作系统等）
  *               为此，SparkStreaming在容错存储系统中需要足够多的checkpoint来恢复故障。
  * @author: Lijiaqi
  * @create: 2019-03-13 13:30
  **/
object Checkpoint {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(Checkpoint.getClass.getName).setMaster("local")

    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.checkpoint("/path")

    /**
      * checkpoint包含两种类型：
      * Metadata checkpoint：我们可以将这些数据保存到分布式的容错存储上，例如HDFS，SparkStreaming将会使用这些数据进行失败任务的恢复
      * MetaData包含了：Configuration data、DStream operation、incomplete batch的数据
      * Data checkpoint：将产生的RDD保存到可靠地存储系统，这在跨越多个batch时处理组合数据的状态转换时是非常必要的。
      * 在一些转换操作中产生的RDD通常会对上一个batch中RDD依赖，随着时间的增长依赖链将会越来越长，这要恢复时所用
      * 的时间也将会是越来越长，这里建议定时将中间状态的RDD存储到HDFS中，切断过长的依赖链。
      * 这里区分一下，Metadata checkpoint主要是用来恢复Driver的故障，而Data checkpoint主要是为了在状态装换操作中使用
      */

    /**
      * checkpoint在以下情况下必须使用:
      * 使用状态转换操作,例如updateStateByKey和reduceByKeyAndWindow，使用时必须同checkpoint文件件
      * 从运行应用程序的driver的故障中恢复，使用metadata checkpoint主要是为了查找任务的进度信息
      * 注意：将RDD保存到外部的可靠存储系统时将会增加spark的处理时间，因此在设置checkpoint周期时要谨慎，过短将会降低整个系统的吞吐量
      * 周期过长将会产生长依赖和任务数量的显著增加。对于需要RDD检查点的有状态转换，默认间隔是至少10秒的批处理间隔的倍数。
      * 它可以通过使用dstream.checkpoint（checkpointinterval）进行设置。通常，数据流的检查点间隔为5-10个滑动间隔是一个很好的尝试设置。
      */
  }

}
