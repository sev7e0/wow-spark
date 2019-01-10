package com.lijiaqi.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-09 22:49
  **/
object parquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetTest")
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("src/main/resources/sparkresource/people.json")

    jsonDF.write.format("parquet").mode("overwrite")
      .save("src/main/resources/sparkresource/people.parquet")

    val peopleDf = spark.read.parquet("src/main/resources/sparkresource/people.parquet")

    peopleDf.createOrReplaceTempView("people")
    val namesDF = spark.sql("select name from people where age between 13 and 19")

    namesDF.show()
  }
}
