package com.sev7e0.spark.core.scala

import org.apache.spark._

//统计每行出现的次数scala版本
object LineCount_Scala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("LineCount_Scala")

    val context = new SparkContext(conf)

    //注意路径格式
    val lineRdd = context.textFile("file:///Users/sev7e0/workspace/idea-workspace/sparklearn/src/main/resources/sparkresource/people.txt")

    val lineCount = lineRdd.map(line => (line, 1)).reduceByKey(_ + _)

    lineCount.foreach(line => println(line._1 + ": " + line._2 + " times"))
  }
}
