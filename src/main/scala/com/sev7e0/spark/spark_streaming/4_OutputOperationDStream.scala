package com.sev7e0.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-03-11 15:33
  **/
object OutputOperationDStream {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      print("input hostname and port")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("OutputOperationDStream").setMaster("local")
    val context = new StreamingContext(conf, Seconds(1))

    val wordDStream = context.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK)

    /**
      * foreachrd是一个功能强大的方法，允许数据发送到外部系统。在开发中有一些经常出错的点需要注意，正确的使用方式将会提升spark的效率
      */
    /**
      * 写入外部系统意味着需要创建连接对象,使用它们进行数据的发送,因此，开发者大意的在driver上创建了一个连接对象，并使用他来进行数据的传递，
      * 但此时并不能进行数据的发送。
      */
    wordDStream.foreachRDD(rdd => {
      /**
        * 这是不正确的，因为这要求将连接对象序列化并从驱动程序发送到工作程序。这样的连接对象很少可以跨机器进行传输。
        * 此错误可能表现为序列化错误（连接对象不可序列化）、初始化错误（连接对象需要在工作区初始化）等。正确的解决方案是在工作区创建连接对象。
        */
      val connection = new createNewConnection()
      rdd.foreach(record => {
        connection.send(record)
      })
    })

    /**
      * 通常在创建连接对象时有很大的时间和资源开销，因此没有必要为每条记录去创建一个连接然后再销毁他.
      */
    wordDStream.foreachRDD(rdd => {
      rdd.foreach(record => {
        val connection = new createNewConnection()
        connection.send(record)
        connection.close()
      })
    })

    /**
      * 一个更好的解决方案就是使用rdd的foreachPartition方法
      * 为每个分区创建一个连接对象，用此对象发送该partition的数据，详细见下一条实现方式。
      */
    wordDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val connection = new createNewConnection
        partition.foreach(record => {
          connection.send(record)
        })
        connection.close()
      })
    })

    /**
      * 该种方式解决掉了，重复创建和关闭连接对象是产生的消耗
      * 最后的优化方案就是使用连接池，这样就可以将一个连接对象重复使用，从而可以使其夸RDD以及batch的使用，进一步减小开销
      */
    wordDStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val connect = createNewConnectionPool.getConnect
        partition.foreach(records => {
          connect.send(records)
        })
        createNewConnectionPool.returnConnection(connect)
      })
    })

    /**
      * 请注意，池中的连接应该根据需要惰性地创建，如果不使用一段时间，则会超时。这实现了向外部系统最有效地发送数据。
      */
    /**
      * 数据流被输出操作延迟地执行，就像RDD被RDD操作延迟地执行一样。具体来说，数据流输出操作中的RDD操作强制处理接收到的数据。
      * 因此，如果您的应用程序没有任何输出操作，或者有像dstream.foreachrdd（）这样的输出操作，但其中没有任何RDD操作，则不会执行任何操作。系统只需接收并丢弃数据。
      *
      * 默认情况下，输出操作一次执行一次。它们按照应用程序中定义的顺序执行
      */


  }


}

object createNewConnectionPool {
  lazy val getConnect = new createNewConnection

  def returnConnection(connect: createNewConnection) = ???
}

class createNewConnectionPool {
}

class createNewConnection extends Serializable {
  def close(): Unit = ???

  def send(record: String): Nothing = ???
}