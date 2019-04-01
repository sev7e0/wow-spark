package com.sev7e0.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DiscretizedStreams {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DiscretizedStreams").setMaster("local[2]")
    val streamContext = new StreamingContext(conf, Seconds.apply(1))
    println("Discretized Streams (DStreams)")

    /**
      * Discretized Stream 或者叫DStream是由SparkStreaming提供的基本抽象,代表了连续不断的流式数据,可以是输入进来的数据,亦可以是经过
      * 转换操作的流数据.在DStream内部其实就是一系列连续的RDD,是Spark对不可变得分布式数据集的抽象,每个RDD在DStream中是归属于一个固定间隔批次.
      */
    /**
      * 在DStream中的任何转换操作都将转换为对底层RDDS的操作.例如像WordCount中的flatmap操作,最后就是对每个批次中的RDDs分别进行操作.
      * 底层的RDDs转换操作是由Spark Engine提供的计算,对于DStream的操作Spark隐藏了细节并提供了高级的API方便开发者使用.
      */
    println("Input DStreams and Receivers")

    /**
      * input DStream是表示从流源接收的输入数据流的DStream.在示例中lines就是一个DStream,它代表从netcat中接收到的流数据.每个inputDStream
      * 关联一个Receiver对象负责接收数据并存储在Spark内存中进行处理.
      */
    //Spark提供了两种内置的流式数据源
    /**
      * Basic sources：StreamingContext API中直接提供的源。示例：文件系统和套接字连接。
      * NetworkWordCount中已经看到从TCP scoket获取数据.另一种就是File System,该文件系统可以包含多种类型(HDFS,S3,NFS等)
      * * 文件流不需要运行接收器，因此不需要为接收文件数据分配任何内核。
      * 创建DStream如下:
      */
    val fileDStream = streamContext.fileStream("dataDirectory")
    val textDStream = streamContext.textFileStream("textDirectory")

    /**
      * 如何监控目录:
      * http://spark.apache.org/docs/latest/streaming-programming-guide.html#how-directories-are-monitored
      */
    /**
      * 使用对象存储作为数据源:
      *   1.在一些文件系统中在更改文件时,在一开始创建输出流式就修改文件更新时间为当前日期,所以在一个文件打开后,在输出流还没结束之前就可能已经
      * 存储在了DStream中,而后(文件更新时间)写入的数据将会被忽略,将会导致数据丢失.
      *   2.解决办法是就是将更新文件写入到一个未被监控的文件夹目录,在文件更新完成关闭输出流后,将其更改为目标目录,如果重命名的文件在其创建窗口期间出现在扫描的目标目录中，则将获取新数据。
      *   3.应该仔细测试对象存储行为是否与spark流所期望的一致.
      */

    /**
      * Advanced Sources: Kafka, Kinesis and Flume
      * http://spark.apache.org/docs/latest/streaming-programming-guide.html#advanced-sources
      */

    /**
      * Custom Sources: Spark支持自定义输入源,需要实现receiver,这样就能实现接收自定义数据源并将其推入到Spark中,详细实现如下:
      * http://spark.apache.org/docs/latest/streaming-custom-receivers.html
      */

    /**
      * receiver的可靠性:
      * 由于输入源可能需要进行传输过的数据进行确认,例如像kafka,flume这种,还有的是不需要进行确认的例如普通的文件系统,
      * 这使得receiver产生了两种情况:
      *     1.可靠的receiver:该receiver会在数据接收,并在spark中做好副本后返回确认信息.
      *     2.不可靠的receiver:该receiver不会返回确认信息,可用于不需要返回信息的数据源,或者为了简化实现逻辑使用给可靠的输入源(输入源本身就是可靠的,不需要确认)
      */
  }
}
