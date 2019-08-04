package com.sev7e0.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

class SecondarySort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SecondarySort_Scala")
    val context = new SparkContext(conf)
    val linesRdd = context.textFile("file:///home/sev7e0/DataSets/secondarysort.txt")

    val mapRdd = linesRdd.map { line =>
      (
        new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
        line)
    }
    mapRdd.sortByKey().map(rdd => rdd._2).foreach(rdd => {
      println(s"rdd = $rdd")
    })

  }
}
