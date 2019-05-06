package com.sev7e0.spark.delta

import org.apache.spark.sql.SparkSession

object QuickStart {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeltaQuickStart")
      .master("local")
      .getOrCreate()

    /**
      * 可以直接将DataFrame创建成Delta table
      */
    val data = spark.range(0, 5)

    data.write
      .format("delta")
      .mode(saveMode = "overwrite")
    data.show()


    /**
      * delta支持标准的DataFrame API
      */
    val deviceFrame = spark
      .read
      .json("src/main/resources/sparkresource/device.json")

    deviceFrame.write
      .format(source = "delta")
      .partitionBy(colNames = "time")
      .mode(saveMode = "overwrite")
      .save(path = "temp/delta-table/device")
    deviceFrame.show()

    /**
      * 可以根据指定的delta文件，创建DF
      */
    val readDeltaDF = spark.read
      .format(source = "delta")
      .load("temp/delta-table/device")
    readDeltaDF.show(2)


    /**
      * 查询指定版本的delta快照数据
      */
    val readSpieclVersion = spark.read
      .format(source = "delta")
      .option("versionAsOf", 0)
      .load("temp/delta-table/device")
    readSpieclVersion.show(3)





    //流数据处理

    /**
      * 将流式数据写入到Delta Lake table中
      */
    val streamingDf = spark.readStream
      .format("rate")
      .load()
    val query = streamingDf.selectExpr("value as id")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "temp/checkpoint")
      .option("mergeSchema", "true")
      .start("temp/delta-table/device")
    query.awaitTermination()

    /**
      * 在向table写入的同时你可以再启动一个流query，
      * 此时写入操作对数据的改变将会被查询操作接收。
      *
      *
      * 该操作要在一个单独的任务中启动
      */

    spark.readStream.format("delta")
      .load("temp/delta-table/device")
      .writeStream
      .format("console")
      .start()

  }
}
