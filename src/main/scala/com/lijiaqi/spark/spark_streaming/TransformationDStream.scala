package com.lijiaqi.spark.spark_streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformationDStream {

  def main(args: Array[String]): Unit ={

    if (args.length<2) {
      println("error")
      System.exit(1)
    }

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationDStream")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    streamingContext.checkpoint(args(2))

    //val socketLines: ReceiverInputDStream[String] = streamingContext.socketTextStream(args(0),args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    /**
      * textFileStream使用的注意事项:
      *   SparkStreaming需要读取流式的数据，而不能直接从文件夹中创建。只能接受流式数据,你可以使用echo命令进行写入数据,
      *   这样就能读取到数据的变化了.(echo aaa > a.txt)
      */
    val lines = streamingContext.textFileStream(args(1))
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))

    val newRDD: RDD[(String, Int)] = streamingContext.sparkContext.parallelize(List(("spark",1), ("Java", 5)))

    val updateStatePairs: DStream[(String, Int)] = pairs.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(streamingContext.sparkContext.defaultParallelism),
      rememberPartitioner = true,
      newRDD)

    updateStatePairs.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }

  def updateFunction(newValues:Seq[Int], runningCount: Option[Int]): Option[Int]={
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
  }
  val newUpdateFunc: Iterator[(String, Seq[Int], Option[Int])] => Iterator[(String, Int)] = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.flatMap(t => updateFunction(t._2, t._3).map(s => (t._1, s)))
  }

}