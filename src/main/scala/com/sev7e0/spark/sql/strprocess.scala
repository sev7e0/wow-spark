package com.sev7e0.spark.sql

import org.apache.spark.sql.SparkSession

object strprocess {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("strprocess").master("local").getOrCreate()

    val strRdd = session.sparkContext.textFile("\\Users\\lijiaqi-yd\\Desktop\\ZZ-FJMX-1.txt")

    strRdd.flatMap(str => str.split(" ")).collect().foreach(str => if(str!= "") print("\""+str.trim+"\","))
  }
}
