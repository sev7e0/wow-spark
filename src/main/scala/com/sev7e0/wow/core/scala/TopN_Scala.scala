package com.sev7e0.wow.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: wow-spark
  * @description:
  * @author: Lijiaqi
  * @create: 2018-11-16 01:06
  **/
object TopN_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("topnScala").setMaster("spark://spark01:7077")
    conf.set("spark.jars", "target/wow-spark-1.0-SNAPSHOT.jar")
    val context = new SparkContext(conf)

    val textRDD = context.textFile("hdfs://spark01:9000/spark/access.log")

    val filterRdd = textRDD.filter(line => line.split(" ").length > 10)
//    filterRdd.take(300).foreach(println)
    val mapRDD = filterRdd.flatMap(_.split(" ")(0)).map((_,1))

    val reduceRDD = mapRDD.reduceByKey(_+_)

    val sortRDD = reduceRDD.sortBy(_._2)

    sortRDD.take(5).foreach(println)

    sortRDD.saveAsTextFile("hdfs://spark01:9000/spark/topn-e")

    context.stop()
  }

}
