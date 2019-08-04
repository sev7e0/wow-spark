package com.sev7e0.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Action 操作也是Spark 任务真正执行的操作
 * 在 Action 方法内部调用了 SparkContext 的 runJob方法,进行 job 的提交
 *
 */
object A_2_ActionOperation_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName(A_2_ActionOperation_Scala.getClass.getName)
    val context = new SparkContext(conf)
    collect(context)
  }

  /**
   * collect将所有的元素放入到这个rdd中，
   * 只有在数据量小的时候才可以使用，因为
   * 他们都是被装入内存的
   *
   * @param context
   */
  def collect(context: SparkContext): Unit = {
    val ints = Array(19, 8, 7, 6, 5, 4, 3, 2, 1)
    val array = context.parallelize(ints, 5).map(_ * 2).collect()
    for (num <- array) {
      println(s"num = ${num}")
    }
  }

  /**
   * reduce 使用指定的交换和结合二元运算符减少此RDD的元素。
   *
   * @param context
   */
  def reduce(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val num = context.parallelize(ints).persist().reduce(_ + _)
    println(num)
  }

  /**
   * count 返回RDD中元素的数量
   *
   * @param context
   */
  def count(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val count = context.parallelize(ints).count()
    println(count)
  }

  /**
   * 遍历这个RDD中的所有元素
   *
   * @param context
   */
  def foreach(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    context.parallelize(ints, 5).foreach(println(_))
  }


  /**
   * 取RDD的第一个num元素。它首先扫描一个分区，然后使用该
   * 分区的结果来估计满足限制所需的额外分区的数量。
   *
   * @param context
   */
  def take(context: SparkContext): Unit = {
    val ints = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val arrays = context.parallelize(ints).take(5)
    for (num <- arrays) {
      println(num)
    }
  }

}














