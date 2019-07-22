package com.sev7e0.spark.spark_streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

object TransformationDStream {

  def main(args: Array[String]): Unit = {

    //    if (args.length<2) {
    //      println("error")
    //      System.exit(1)
    //    }

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationDStream")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    //使用updateStateByKey 需要配置checkpoint目录详细请参考 http://spark.apache.org/docs/latest/streaming-programming-guide.html#checkpointing
    streamingContext.checkpoint("src/main/resources/sparkresource/")

    /**
      * 这里使用tcp socket作为输入流，nc -lk 8888 Windows中可以使用 ncat -lk 8888
      */
    val lines = streamingContext.socketTextStream("localhost", 8888)
    /**
      * textFileStream使用的注意事项:
      * SparkStreaming需要读取流式的数据，而不能直接从文件夹中创建文件写入数据。你可以使用echo命令进行写入数据,
      * 这样就能读取到数据的变化了.(echo aaa > a.txt)
      */
    //    val lines = streamingContext.textFileStream(args(1))
    val words: DStream[String] = lines.flatMap(_.split(" "))

    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    val newRDD: RDD[(String, Int)] = streamingContext.sparkContext.parallelize(List(("spark", 1), ("Java", 5)))

    val updateStatePairs: DStream[(String, Int)] = pairs.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner(streamingContext.sparkContext.defaultParallelism),
      rememberPartitioner = true,
      newRDD)

    updateStatePairs.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
  }

  val newUpdateFunc: Iterator[(String, Seq[Int], Option[Int])] => Iterator[(String, Int)] = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.flatMap(t => updateFunction(t._2, t._3).map(s => (t._1, s)))
  }

}