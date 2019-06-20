package com.sev7e0.spark.structured_streaming

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-27 18:31
  **/
object A_2_JoinOperation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(A_2_JoinOperation.getClass.getName)
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("")
    val frame = spark.readStream.format("json")
      .option("", "")
      .load()

    frame.join(jsonDF, "type")
  }

}
