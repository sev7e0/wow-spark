package com.sev7e0.spark.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-03-12 15:58
  **/
object DataFrameAndSql {

  def main(args: Array[String]): Unit = {

    StreamingLogger.setLoggerLevel()

    val conf = new SparkConf().setMaster("local").setAppName(DataFrameAndSql.getClass.getName)
    val context = new StreamingContext(conf, Seconds(1))

    val wordsDS = context.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)

    val words = wordsDS.flatMap(_.split(" "))

    /**
      * 在Streamingdata中你可以很轻松的使用DataFrame和sql,创建SparkSession时需要使用StreamingContext中的SparkContext创建，这样在重启任务
      * 时，才能够进行任务的恢复，本示例中加载了一个单例的SparkSession。
      */
    words.foreachRDD(rdd => {

      val sparkSession = SparkSessionSingleton.getInstance(conf)
      import sparkSession.implicits._

      val wordFrame = rdd.toDF("words")

      wordFrame.createOrReplaceTempView("words")

      /**
        * 这里我们用DF创建的table进行查询,使用sql实现wordcount的功能
        */
      val frame = sparkSession.sql("select word, count(*) as total from words group by word")

      frame.show()
    })

    context.start()

    context.awaitTermination()

  }

  /**
    * 当然你也可以使用异步线程去进行sql查询,但是你要确保你的流数据量足够进行查询。否则，不知道任何异步SQL查询的streamingcontext将在查询完成之前删除旧的流数据。
    * 举例说如果你向查询最后一个batch，但是你这个查询需要五分钟的，可以使用streamingContext.remember(5min)
    */


}

object SparkSessionSingleton {
  @transient private var instance: SparkSession = _

  def getInstance(conf: SparkConf): SparkSession = {
    if (instance.==(null)) {
      instance = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
    }
    instance
  }
}


