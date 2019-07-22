package com.sev7e0.spark.spark_streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: spark-learn
  * @description: http://spark.apache.org/docs/latest/streaming-programming-guide.html#accumulators-broadcast-variables-and-checkpoints
  * @author: Lijiaqi
  * @create: 2019-03-13 14:46
  **/
object RecoverableNetworkWordCount {

  def main(args: Array[String]): Unit = {

    StreamingLogger.setLoggerLevel()

    val conf = new SparkConf().setMaster("local").setAppName(RecoverableNetworkWordCount.getClass.getName)
    val context = new StreamingContext(conf, Seconds(1))

    val linesDS = context.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_2)

    val wordsCounts = linesDS.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    wordsCounts.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      val blackList = WordBlackList.getInstance(context.sparkContext)

      val accumulator = DropWordCounter.getInstance(context.sparkContext)

      val str = rdd.filter { case (word, count) =>
        if (blackList.value.contains(word)) {
          accumulator.add(count)
          false
        } else {
          true
        }
      }.collect().mkString("[", ", ", "]")
      println(s"str = $str")
    })
  }


}

object WordBlackList {

  @volatile private var instance: Broadcast[Seq[String]] = _

  def getInstance(context: SparkContext): Broadcast[Seq[String]] = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val blackList = Seq("a", "b", "c")
          instance = context.broadcast(blackList)
        }
      }
    }
    instance
  }

}

object DropWordCounter {
  @volatile private var instance: LongAccumulator = _

  def getInstance(context: SparkContext): LongAccumulator = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = context.longAccumulator("WordCount")
        }
      }
    }
    instance
  }
}