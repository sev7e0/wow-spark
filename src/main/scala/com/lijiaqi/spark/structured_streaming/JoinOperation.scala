package com.lijiaqi.spark.structured_streaming

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-27 18:31
  **/
object JoinOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(JoinOperation.getClass.getName)
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("")
    val frame = spark.readStream.format("json")
      .option("", "")
      .load()

    frame.join(jsonDF,"type")
  }

}
