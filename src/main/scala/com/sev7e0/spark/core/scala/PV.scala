package com.sev7e0.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

object PV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")setAppName(PV.getClass.getName)
    val context = new SparkContext(conf)


    context.setLogLevel("info")

    val textRDD = context.textFile("src/main/resources/sparkresource/kv1.txt")

    val sum = textRDD.count()

    println(sum)

    context.stop()
  }
}
