package com.sev7e0.spark.structured_streaming

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

/**
  * no support for execution
  */
object A_7_WriteStream {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(A_7_WriteStream.getClass.getName)
      .master("local")
      .getOrCreate()
    val streamDF = session.readStream.load()


    /**
      * 使用writeStream则需要指定一些规则和模式
      *   1.queryName:非必须,指定一个唯一的名字,能够方便识别
      *   2.outputMode:非必须,输出模式,当前支持 Append mode(默认)/Complete mode/Update mode(从2.1.1开始)
      *   3.trigger:非必须,如未设置触发器,系统将在处理完成后立即检查数据可用性,如超时导致错过触发器,将在完成后立即触发
      *   4.format:生成格式,parquet/json/csv/kafka...
      */
    // File Sink ("orc", "json", "csv")
    streamDF.writeStream
      .queryName("test")
      //      .trigger()
      .outputMode("complete")
      .format("parquet")
      .option("path", "/")
      .partitionBy()
      .start()

    // Kafka Sink
    streamDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark01:2133,spark02:2133")
      .option("topic", "updates")
      .start()

    //Console Sink(for debug)
    streamDF.writeStream
      .queryName("console")
      .format("console")
      .start()

    // Memory Sink Console Memory谨慎使用,仅适合数据量小的情况下适用
    streamDF.writeStream
      .queryName("memory")
      .format("memory")
      .start()
    /**
      * 一些sink不支持容错,不能保证数据的持久性,仅适合debug使用
      * 参考链接:
      * http://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
      */

    val deviceDF = session.readStream.load()

    //将接收数据打印屏幕
    deviceDF.writeStream
      .format("console")
      .start()

    deviceDF.writeStream
      .format("parquet")
      .option("checkpointLocation", "/path/checkpoint/dir")
      .option("path", "/path/output/dir")
      .start()

    val aggregationDF = deviceDF.groupBy("device").count()

    //聚合后数据结果到console
    aggregationDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    //默认查询name会作为table name
    aggregationDF.writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("device")
      .start()
    //根据生成的table进行查询
    session.sql("select * from device").show()

    /**
      * Foreach Sink
      *
      * ForeachBatch 允许指定在流查询的每个微批处理的输出数据上执行的函数。
      */
//    streamDF.writeStream.foreachBatch((a, b) => {
//      // 打印出每个微批
//      a.show()
//      print(b)
//    })

    /**
      * 当foreachBatch不能满足时(不存在对应的批处理data writer,或者连续处理模式),就可以将处理逻辑使用foreach代替
      * foreach参数指定了ForeachWriter抽象类,该抽象类中提供了三个抽象方法,open,process,close,需要子类实现,
      *
      * note:
      *   1.每个ForeachWriter实现的副本针对了每一次遍历的partition(分布式情况下产生的一个数据分区)
      *   2.该对象必须实现serializable,因为每个任务将获得所提供对象的一个新的序列化反序列化副本。
      *   3.每一个partition对应了一个id,每个Batch也对应了一个epoch_id,方法调用顺序为open->process->close
      *   4.只有在open成功返回后才可以调用close,除非jvm或者python中途崩溃
      *
      * partition_id, epoch_id主要是为了保证exactly-once语义,但如果在连续模式(continuous mode)下则无法保证
      */
    streamDF.writeStream.foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = {
        //打开连接
        true
      }

      override def process(value: Row): Unit = {
        //处理过程
      }

      override def close(errorOrNull: Throwable): Unit = {
        //关闭连接
      }
    }).start()

    /**
      * Triggers:
      *   1.default:在流数据查询中如果未指定trigger类型,则使用默认方式micro-batch mode进行,每个micro-batch在前一个micro-batch完成后立即执行
      *   2.fixed interval micro-batch:查询以micro-batch模式运行,根据设定的间隔进行触发
      * 如果前一个提前完成,这下一个micro-batch将会在指定的时间间隔后执行
      * 如果前一个超过了时间间隔未完成,则下一个会在前一个执行完成后立即执行
      * 如果没有数据到达,不会启动micro-batch
      *   3.one-time micro-batch:
      *   4.Continuous with fixed checkpoint interval:查询将以新的低延迟连续处理模式执行
      */
    import org.apache.spark.sql.streaming.Trigger

    //不指定trigger的情况下使用default
    streamDF.writeStream
      .format("console")
      .start()

    //micro-batch设置为2秒间隔
    streamDF.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    //one-time trigger
    streamDF.writeStream
      .format("console")
      .trigger(Trigger.Once())
      .start()

    //具有一秒检查点间隔的连续触发器
    streamDF.writeStream
      .format("console")
      .trigger(Trigger.Continuous("1 seconds"))
      .start()
  }

}
