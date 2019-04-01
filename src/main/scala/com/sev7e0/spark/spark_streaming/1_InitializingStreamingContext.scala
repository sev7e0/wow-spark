package com.sev7e0.spark.spark_streaming

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object InitializingStreamingContext {
  def main(args: Array[String]): Unit = {
    /**
      * 相关参数:
      * setMaster:使用local是指定了本地运行模式,这只是为了本地测试,实际上,运行在集群上时,不应该在程序中采用硬编码的方式,而是在使用spark-submit时指定
      * setAppName:该名字是应用在集群的ui界面显示的名字
      */
    val sparkConf = new SparkConf().setMaster("local").setAppName("InitializingStreamingContext")

    val sparkContext = new SparkContext(sparkConf)

    /**
      * StreamingContext对象可以通过已经存在的SparkContext对象进行创建
      * 亦可以由SparkConf对象作为参数进行创建
      * 在设置批处理时间间隔时( Seconds(1)),一定要考虑到集群的性能,具体可以参考下面性能调优的连接
      * http://spark.apache.org/docs/latest/streaming-programming-guide.html#setting-the-right-batch-interval
      */
    val streamingContext = new StreamingContext(sparkContext, Seconds(1))
    //    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    /**
      * 在定义好context对象后,接下来应该实现以下操作:
      *   1.定义一个输入源作为输入的离散流(DStream)
      *   2.通过对DStreams应用转换和输出操作来定义流计算
      *   3.使用streamingContext.start()开始接收并处理数据
      *   4.使用streamingContext.awaitTermination()等待处理任务结束
      *   5.处理过程可以使用streamingContext.stop()手动停止
      */
    /**
      * 注意事项:
      *   1.context一旦启动不能够在添加或设置流计算,一旦停止不能在重新启动
      *   2.JVM中只有一个StreamingContext可以同时活动
      *   3.默认情况下StreamingContext使用stop()也会同时将SparkContext停止,如果不想停止SparkContext则需要context.stop(false)
      *   4.同一个SparkContext 可以创建多个StreamingContext,只要上一个StreamingContext停止后,下一个StreamingContext就可以创建.
      */


  }

}
