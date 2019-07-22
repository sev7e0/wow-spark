package com.sev7e0.spark.sql

import org.apache.spark.sql.SparkSession

case class Person(id:Int, name: String, age: Int)
case class Result(id:Int, result:Int)
object A_4_JoinDF {

  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder().appName("SimpleApp").master("local").getOrCreate()
    val list = List((1,"mike", 24), (2,"joe", 34), (3,"jack", 55))
    val res = List((1, 80), (2, 50), (3, 100))

    import sqlContext.implicits._

    val infoRdd = sqlContext.sparkContext.parallelize(list)
    val resRdd = sqlContext.sparkContext.parallelize(res)
    val infoDF = infoRdd.map(info => Person(info._1, info._2, info._3)).toDF()
    val resDF = resRdd.map(res => Result(res._1, res._2)).toDF()
    //将两个DataFrame合并,并需要指定合并条件
    val joinDF = infoDF.join(resDF,"id")
    joinDF.createOrReplaceTempView("people")
    val frame = sqlContext.sql("select * from people where age>30")

    frame.show()
  }

}
