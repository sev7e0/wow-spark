package com.lijiaqi.spark.core

import org.apache.spark.sql.SparkSession

object LineCountLocal_Scala extends AutoCloseable{

  private val session: SparkSession = SparkSession.builder().appName("LineCountLocal_Scala").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    val context = session.sparkContext

    val lines = context.textFile("file:/home/sev7e0/bigdata/spark-2.2.0-bin-hadoop2.7/README.md")
    val res = lines.map(line => line.length).reduce(_+_)
    println(s"res = ${res}")
  }

  override def close(): Unit = {
    session.close()
  }
}
