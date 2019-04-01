package com.sev7e0.spark.core.scala

import org.apache.spark._

//统计每行出现的次数scala版本
object LineCount_Scala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LineCount_Scala")

    val context = new SparkContext(conf)

    val lineRdd = context.textFile("file:///home/sev7e0/DataSets/linecount.txt")

    val lineCount = lineRdd.map(line => (line, 1)).reduceByKey(_ + _)

    lineCount.foreach(line => println(line._1 + ": " + line._2 + " times"))
  }
}
