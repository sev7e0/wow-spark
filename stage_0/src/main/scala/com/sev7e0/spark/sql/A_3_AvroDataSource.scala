package com.sev7e0.spark.sql

import org.apache.spark.sql.SparkSession

/**
  *  1.Avro数据格式是在2.4版本后添加
  *  2.支持读取avro数据格式和写入数据格式
  *  3.在针对流式数据是(kafka),使用更方便,Avro中的record可以保存一些卡夫卡的元数据,时间戳\偏移量等
  *  4.使用from_avro()和to_avro()
  *
  */

object A_3_AvroDataSource {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("AvroDataSource").master("local").getOrCreate()
    val userDF = sparkSession.read.format("avro").load("src/main/resources/sparkresource/users.avro")

    userDF.select("name", "favorite_color").write
      .format("avro")
      .mode("overwrite")
      .save("src/main/resources/sparkresource/usertemp.avro")

    sparkSession.read
      .format("avro")
      .load("src/main/resources/sparkresource/usertemp.avro")
      .select().show()
  }
}
