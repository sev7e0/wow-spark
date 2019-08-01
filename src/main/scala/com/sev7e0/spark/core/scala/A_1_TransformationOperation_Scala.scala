package com.sev7e0.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Spark中相关的转换操作,delay operation,只有在Action(参看A_2_ActionOperation_Scala)
 * 操作之后才会真实的执行.
 *
 * Spark 根据操作属性分别 RDD 之间的依赖属性,再根据依赖属性进行 stage 的划分,最终将 stage
 * 封装成 job 进行任务的提交.
 *
 * 依赖属性:
 *    宽依赖(ShuffleDependency): 是指当前 rdd 的父RDD 被多个 RDD 引用
 *    窄依赖(NarrowDependency): 是指当前 rdd 的父RDD 只被一个 RDD 所引用
 *
 * 详细参看我的博客:
 */
object A_1_TransformationOperation_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(A_1_TransformationOperation_Scala.getClass.getName)
    val context = new SparkContext(conf)
    flatMap(context)
    map(context)
  }

  /**
   * 通过向这个RDD的所有元素应用一个函数来返回一个新的RDD。
   *
   * @param context
   */
  def map(context: SparkContext): Unit = {
    val listRdd = Array(1, 2, 3, 5, 8, 6)
    context.parallelize(listRdd).map(_ * 2).foreach(println(_))
  }

  def flatMap(context: SparkContext): Unit = {
    val listRdd = Array("hello you ", "hello java", "hello leo")
    context.parallelize(listRdd).flatMap(_.split(" ")).foreach(println(_))
  }

  /**
   * 过滤掉不符合要求的元素，返回一个新的 rdd
   *
   * @param context
   */
  def filter(context: SparkContext): Unit = {
    val listRdd = Array(1, 2, 3, 5, 8, 6, 7, 8, 9, 10)
    context.parallelize(listRdd).filter(_ % 2 != 0).foreach(println(_))
  }

  def gropByKey(context: SparkContext): Unit = {
    val listRdd = Array(new Tuple2[String, Integer]("class1", 50),
      new Tuple2[String, Integer]("class1", 80),
      new Tuple2[String, Integer]("class2", 65),
      new Tuple2[String, Integer]("class3", 580),
      new Tuple2[String, Integer]("class1", 75))
    context.parallelize(listRdd).groupByKey().foreach(rdd => {
      println(rdd._1);
      for (score <- rdd._2) println(score)
    })
  }

  def join(context: SparkContext): Unit = {
    val scoreList = Array(new Tuple2[Integer, Integer](1, 25),
      new Tuple2[Integer, Integer](2, 89),
      new Tuple2[Integer, Integer](3, 100),
      new Tuple2[Integer, Integer](4, 75),
      new Tuple2[Integer, Integer](5, 0))
    val nameList = Array(new Tuple2[Integer, String](1, "leo"),
      new Tuple2[Integer, String](2, "json"),
      new Tuple2[Integer, String](3, "spark"),
      new Tuple2[Integer, String](4, "fire"),
      new Tuple2[Integer, String](5, "samsung"))
    context.parallelize(nameList)
      .join(context.parallelize(scoreList))
      .foreach(scoreName => {
        println(scoreName._1)
        println(scoreName._2._1)
        println(scoreName._2._2)
      })
  }

  def cogroups(context: SparkContext): Unit = {
    val scoreList = Array(new Tuple2[Integer, Integer](1, 25),
      new Tuple2[Integer, Integer](2, 89),
      new Tuple2[Integer, Integer](3, 100),
      new Tuple2[Integer, Integer](3, 75),
      new Tuple2[Integer, Integer](1, 0))
    val nameList = Array(new Tuple2[Integer, String](1, "leo"),
      new Tuple2[Integer, String](2, "json"),
      new Tuple2[Integer, String](3, "spark"),
      new Tuple2[Integer, String](2, "fire"),
      new Tuple2[Integer, String](1, "samsung"))

    context.parallelize(nameList)
      .cogroup(context.parallelize(scoreList))
      //scala中输入多行需要使用 { } 进行包裹，否则只能使用一行处理
      .foreach(score => {
        println(score._1);
        println(score._2._1);
        println(score._2._2);
        println("----------")
      })

  }
}














