package com.sev7e0.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2018-11-16 01:06
  **/
object TopN_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("topnScala").setMaster("local")
    val context = new SparkContext(conf)

    val pairRdd = context.textFile("/Users/sev7e0/dataset/topn.txt").map(line => Tuple2(Integer.valueOf(line), line))

    val integer = pairRdd.sortByKey(false).map(rdd => rdd._1).take(3)

    integer.foreach(integer => println(s"integer = $integer"))


  }

}
