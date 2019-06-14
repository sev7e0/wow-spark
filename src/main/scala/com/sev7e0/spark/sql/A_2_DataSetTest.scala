package com.sev7e0.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-02 23:57
  **/

object A_2_DataSetTest {
  case class Person( name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataSetTest")
      .master("local")
      .getOrCreate()
    import sparkSession.implicits._
    val dataSet= Seq(Person("lee",18)).toDS()
    dataSet.show()
    val frame = Seq(1,5,7).toDS()
    val inns = frame.map(_ + 1).collect()
    inns.foreach(print(_))
  }

}
