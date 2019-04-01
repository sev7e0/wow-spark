package com.sev7e0.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @description: TODO
  * @author: Lijiaqi
  * @version: 1.0
  * @create: 2019-03-08 10:50
  **/
object TransformationsDStream {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("must input hostname and port")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("transformFilterDStream")
    val context = new StreamingContext(conf, Seconds(1))

    val socketDS = context.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_2)

    val words = socketDS.flatMap(_.split(" "))

    val wordsMap = words.map(s => (s, 1))

    val wordsReduce = wordsMap.reduceByKey(_ + _)

    /**
      * SparkStreaming支持窗口计算,可以对每个滑动窗口的数据进行计算,在使用时注意两个关键参数设置
      * window length - The duration of the window (3 in the figure). Seconds(10)
      * sliding interval - The interval at which the window operation is performed (2 in the figure). Seconds(5)
      * 这两个参数必须是源DStream的批处理间隔的倍数
      * 该实例中参数的含义是,每五秒统计一次最近十秒的数据量
      */
    val windowsCounts = wordsReduce.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(10), Seconds(5))

    windowsCounts.print()


    val spamInfoRDD = context.sparkContext.parallelize(List(("spark", 1), ("java", 1)))

    val excludeSparkDS = wordsReduce.filter(word => word._1.equals("spark"))

    /**
      * 在SparkStreaming中Transform(及其他，如transformWith)允许在DStream上应用任意的RDD-to-RDD函数。
      * 支持一些在DStream中没有公开的RDD的操作的API,例如在每个batch中可以进行与其他的数据集进行join操作,这是DStream的API中没有公开提供的,
      * 但是可以使用Transform实现,该方法有很多的可能性,可以实现实时数据的清洗.
      */
    /**
      * Transform在每个间隔的批次被调用,这期间使用者可以进行对RDD的更改,例如number of partitions, broadcast variables等.
      */
    val resDS = excludeSparkDS.transform(w => w.join(spamInfoRDD).filter(_._1 == "java"))
    resDS.print()

    /**
      * 在SparkStreaming中支持了两种join操作
      */
    /**
      * 1.Stream-Stream Join
      * val stream1: DStream[String, String] = ...
      * val stream2: DStream[String, String] = ...
      * val joinedStream = stream1.join(stream2)
      */
    /**
      * 2.Stream-DataSet Join
      * val dataset: RDD[String, String] = ...
      * val windowedStream = stream.window(Seconds(20))...
      * val joinedStream = windowedStream.transform { rdd => rdd.join(dataset) }
      * 和上边transform提到的一样,你可以动态修改dataset,因为该操作是固定时间间隔触发,当数据集修改后将会使用最新的数据集
      */

    context.start()

    context.awaitTermination()
  }

}
