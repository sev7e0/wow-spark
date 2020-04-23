package com.sev7e0.wow.sql

import org.apache.spark.sql.SparkSession

case class Person(id:Int, name: String, age: Int)
case class Result(id:Int, result:Int)
object A_4_JoinDF {

  def main(args: Array[String]): Unit = {
    val sqlContext = SparkSession.builder().appName("SimpleApp").master("local").getOrCreate()
    val list = List((1,"mike", 24), (2,"joe", 34), (3,"jack", 55), (4,"mac", 60))
    val res = List((1, 80), (2, 50), (3, 100),(5, 150))

    import sqlContext.implicits._

    val infoRdd = sqlContext.sparkContext.parallelize(list)
    val resRdd = sqlContext.sparkContext.parallelize(res)
    val infoDF = infoRdd.map(info => Person(info._1, info._2, info._3)).toDF()
    val resDF = resRdd.map(res => Result(res._1, res._2)).toDF()
    
    //将两个DataFrame合并,并需要指定合并条件
  
    /**
     * Default `inner`. Must be one of:
     * `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
     * `right`, `right_outer`, `left_semi`, `left_anti`.
     */
    val joinDF = infoDF.join(resDF,"id")
    joinDF.createOrReplaceTempView("people")
    val frame = sqlContext.sql("select * from people")
    frame.show()
    //+---+----+---+------+
    //| id|name|age|result|
    //+---+----+---+------+
    //|  1|mike| 24|    80|
    //|  3|jack| 55|   100|
    //|  2| joe| 34|    50|
    //+---+----+---+------+
  
    val leftJoinDF = infoDF.join(resDF,Seq( "id"), "left")
    leftJoinDF.createOrReplaceTempView("leftPeople")
    val leftF = sqlContext.sql("select * from leftPeople")
    leftF.show()
    //+---+----+---+------+
    //| id|name|age|result|
    //+---+----+---+------+
    //|  1|mike| 24|    80|
    //|  3|jack| 55|   100|
    //|  4| mac| 60|  null|
    //|  2| joe| 34|    50|
    //+---+----+---+------+
    
    val fullJoinDF = infoDF.join(resDF,Seq( "id"), "full")
    fullJoinDF.createOrReplaceTempView("fullPeople")
    val fullF = sqlContext.sql("select * from fullPeople")
    fullF.show()
    //+---+----+----+------+
    //| id|name| age|result|
    //+---+----+----+------+
    //|  1|mike|  24|    80|
    //|  3|jack|  55|   100|
    //|  5|null|null|   150|
    //|  4| mac|  60|  null|
    //|  2| joe|  34|    50|
    //+---+----+----+------+
  }

}
