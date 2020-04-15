package com.sev7e0.wow.core.scala

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test


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
  private val log = Logger.getLogger(A_1_TransformationOperation_Scala.getClass)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(A_1_TransformationOperation_Scala.getClass.getName)
    log.info("")
    val context = new SparkContext(conf)

    mapPartitions(context)
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

  /**
   * 类似于map，但是是在每一个partition上运行.
   * 假设有N个元素，有M个分区，那么map的函数的将被调用N次,
   * 而mapPartitions被调用M次,一个函数一次处理所有分区。
   *
   * 返回一个MapPartitionRDD
   *
   * @param context
   */
  def mapPartitions(context: SparkContext): Unit = {
    val value = context.parallelize(1 to 10, 3)
    //构造function，参数和返回值只能是Iterator类型
    def mapPartitionsFunction(iterator: Iterator[Int]): Iterator[(Int,Int)]={
      var result = List[(Int,Int)]()
      while (iterator.hasNext){
        val i = iterator.next()
          //在list开头插入新的tuple，由于是不可变得，所以生成了一个新的list
          result = result.::(i, i*i)
      }
      result.iterator
    }
    val listValue = context.parallelize(List((1,1), (1,2), (1,3), (2,1), (2,2), (2,3)))
    //向mapPartitions传递function
    val partitionRDD = value.mapPartitions(mapPartitionsFunction)
    partitionRDD.foreach(out=>println(out))
    //(3,9)
    //(2,4)
    //(1,1)
    //(6,36)
    //(5,25)
    //(4,16)
    //(10,100)
    //(9,81)
    //(8,64)
    //(7,49)

    //函数式写法 生成新的(a , b*b)
    val tupleRDD = listValue.mapPartitions(iterator => {
      var resultTuple = List[(Int, Int)]()
      while (iterator.hasNext) {
        val tuple = iterator.next()
        resultTuple = resultTuple.::(tuple._1, tuple._2 * tuple._2)
      }
      resultTuple.iterator
    })
    tupleRDD.foreach(println(_))
    //(9,81)
    //(8,64)
    //(7,49)
    //(2,9)
    //(2,4)
    //(2,1)
    //(1,9)
    //(1,4)
    //(1,1)
  }

  def reduceByKey(context: SparkContext): Unit={
    val value = context.parallelize(1 to 20)
//    value.map(_ => (_,1)).reduceByKey(_ + _)
  }

  /**
   *
   * @param context
   */
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

  def groupByKey(context: SparkContext): Unit = {
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














