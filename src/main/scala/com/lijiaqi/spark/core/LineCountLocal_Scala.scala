package com.lijiaqi.spark.core

import org.apache.spark._

object LineCountLocal_Scala extends AutoCloseable{

//  private val session: SparkSession = SparkSession.builder()
//    .appName("LineCountLocal_Scala")
//    .master("local")
//    .getOrCreate()


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCountLocal_Scala").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("file:/home/sev7e0/bigdata/spark-2.2.0-bin-hadoop2.7/README.md")
    val res = lines.map(line=>line.length).reduce(_+_)
    println(s"res = $res")
  }

  override def close(): Unit = {
//    session.close()
  }
}
