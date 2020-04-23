package com.sev7e0.wow.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @program: wow-spark
 * @description:
 * @author: sev7e0
 * @create: 2018-11-16 01:06
 **/
object TopN_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("topnScala").setMaster("local[2]")
    val context = new SparkContext(conf)
    
    val textRDD = context.textFile("src/main/resources/sparkresource/access.log")
    
    val filterRDD = textRDD.filter(line => line.split("  ").length > 10)
    
    val mapRDD = filterRDD.map(line => {
      val strings = line.split("  ")
      (strings(0), 1)
    })
    
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    
    val sortRDD = reduceRDD.sortBy(_._2, ascending = false)
    
    sortRDD.take(5).foreach(println)
    
    context.stop()
    
    //(163.177.71.12,972)
    //(101.226.68.137,972)
    //(183.195.232.138,971)
    //(111.192.165.229,334)
    //(114.252.89.91,294)
  }
  
}
