package com.lijiaqi.spark.core
import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationOperation_Scala")
    val context = new SparkContext(conf)
    map(context)
  }

  def map(context: SparkContext): Unit ={
    val listRdd = Array(1,2,3,5,8,6)
//    context.parallelize(listRdd).map(_*2).foreach(println(_))
  }


  def filter(context: SparkContext): Unit ={
    val listRdd = Array(1,2,3,5,8,6,7,8,9,10)
//        context.parallelize(listRdd).filter(_%2 != 0).foreach(println(_))
  }

  def flatMap(context: SparkContext): Unit ={
//    val listRdd = Array("hello you ", "hello java", "hello leo")
//    context.parallelize(listRdd).flatMap(_.split(" ")).foreach(println(_))
  }
  def gropByKey(context: SparkContext): Unit ={
//        val listRdd = Array(new Tuple2[String, Integer]("class1", 50),
//          new Tuple2[String, Integer]("class1", 80),
//          new Tuple2[String, Integer]("class2", 65),
//          new Tuple2[String, Integer]("class3", 580),
//          new Tuple2[String, Integer]("class1", 75))
//        context.parallelize(listRdd).groupByKey().foreach(rdd=>{println(rdd._1);for(score <- rdd._2) println(score)})
    //    context.parallelize(listRdd).flatMap(_.split(" ")).foreach(println(_))
  }
}
