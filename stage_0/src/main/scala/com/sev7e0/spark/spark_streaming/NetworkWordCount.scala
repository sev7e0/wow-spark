package com.sev7e0.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("请输入hostname及port")
      System.exit(1)
    }

    // 设置Streaming的名字,以及配置两个本地的执行线程, 创建一个StreamingContext需要的SparkConf对象
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    // 创建context并设置interval时间间隔为1秒
    val context = new StreamingContext(conf, Seconds(1))
    // 使用context可以穿件一个代表来自socket的DStream,这里需要指定hostname,port
    val lines = context.socketTextStream(args(0), args(1).toInt)
    // 名字为lines的DStream代表收到发送进来的数据,每个DStream为一行文本,我们将改行文本按照" "(空格)进行分割成单词
    val words = lines.flatMap(_.split(" "))
    // 将产生的新的DStream->words中的每个单词map成(word, 1)格式的DStream
    words.map(word => (word, 1))
      //根据key,也就是单词进行reduce操作,将会把相同key的value进行想加,
      .reduceByKey(_ + _)
      .print()

    /**
      * 在执行这些行时，Spark流仅设置在启动时会执行的计算，并且还没有启动任何实际的处理。要在设置好所有转换之后启动处理，我们最后调用
      */
    context.start()

    context.awaitTermination()
  }

}
