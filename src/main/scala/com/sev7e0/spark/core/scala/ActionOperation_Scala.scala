package com.sev7e0.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
  * ActionOperation相关方法 Scala版
  */
object ActionOperation_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ActionOperation_Scala")
    val context = new SparkContext(conf)
    reduce(context)
  }

  def reduce(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val num = context.parallelize(ints).reduce(_ + _)
    println(num)
  }

  def count(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val count = context.parallelize(ints).count()
    println(count)
  }

  def collect(context: SparkContext): Unit = {
    val ints = Array(19, 8, 7, 6, 5, 4, 3, 2, 1)
    val array = context.parallelize(ints, 5).map(_ * 2).collect()
    for (num <- array) {
      println(s"num = ${num}")
    }
  }

  def foreach(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    context.parallelize(ints, 5).foreach(println(_))
  }

  def take(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val arrays = context.parallelize(ints).take(5)
    for (num <- arrays) {
      println(num)
    }
  }

}














