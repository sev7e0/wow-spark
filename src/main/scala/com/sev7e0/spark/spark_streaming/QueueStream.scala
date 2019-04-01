package com.sev7e0.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable

object QueueStream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("QueueStream").setMaster("local")

    val ssc = new StreamingContext(conf, Seconds(1))

    //使用queue时推荐加入前缀,方便区分mutable和immutable
    val queue = new mutable.Queue[RDD[Int]]

    val queueDStream = ssc.queueStream(queue)

    val mapDStream = queueDStream.map(m => (m % 10, 1))
    val reduceDStream = mapDStream.reduceByKey(_ + _)

    reduceDStream.print(100)

    ssc.start()

    //随机创建RDD并将其推进queue中
    for (_ <- 1 to 50) {
      queue.synchronized {
        queue += ssc.sparkContext.makeRDD(1 to scala.util.Random.nextInt(100), 10)
      }
      Thread.sleep(1000)
    }

    ssc.stop()
  }

}
