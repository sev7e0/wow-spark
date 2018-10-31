package com.lijiaqi.spark.kafka
import org.apache.spark.sql.SparkSession

object StatStramingApp {

  def main(args: Array[String])= {
    val spark = SparkSession
      .builder()
      .master("spark://localhost:7077")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val dataFrame = spark.read.json("/home/sev7e0/bigdata/spark-2.2.0-bin-hadoop2.7/examples/src/main/resources/people.json")
    dataFrame.show();
    dataFrame.printSchema();
  }

}
