package com.sev7e0.spark.core.scala

import org.apache.spark._

//BroadCast Scala版本
object BroadCastVariable_Scala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("BroadCastVariable_Scala").setMaster("local")
    val context = new SparkContext(conf)

    val array = Array(1, 2, 3, 4, 5, 6, 7, 8)
    val factor = 5
    //设置共享变量
    val broadcast = context.broadcast(factor)

    context.parallelize(array).map(arr => arr * broadcast.value).foreach(args => println(args))
  }

}
