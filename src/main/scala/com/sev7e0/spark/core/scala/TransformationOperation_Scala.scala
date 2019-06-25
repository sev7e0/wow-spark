package com.sev7e0.spark.core.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Spark中相关的转换操作，这些操作都不会被立即执行
  * 只有在action操作后才会执行
  */
object TransformationOperation_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(TransformationOperation_Scala.getClass.getName)
    val context = new SparkContext(conf)
    map(context)
  }

  def map(context: SparkContext): Unit = {
    val listRdd = Array(1, 2, 3, 5, 8, 6)
    context.parallelize(listRdd).map(_ * 2).foreach(println(_))
  }


  def filter(context: SparkContext): Unit = {
    val listRdd = Array(1, 2, 3, 5, 8, 6, 7, 8, 9, 10)
    context.parallelize(listRdd).filter(_ % 2 != 0).foreach(println(_))
  }

  def flatMap(context: SparkContext): Unit = {
    val listRdd = Array("hello you ", "hello java", "hello leo")
    context.parallelize(listRdd).flatMap(_.split(" ")).foreach(println(_))
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
    //        context.parallelize(listRdd)
  }

  def cogroup(context: SparkContext): Unit = {
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














