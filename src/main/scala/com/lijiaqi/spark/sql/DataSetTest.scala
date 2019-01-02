package com.lijiaqi.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-02 23:57
  **/

object DataSetTest {
  case class Person( name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataSetTest")
      .master("local")
      .getOrCreate()
    import sparkSession.implicits._
    val dataFrame = Seq(Person("lee",18)).toDS()
    dataFrame.show()

    val frame = Seq(1,5,7).toDS()
    val inns = frame.map(_ + 1).collect()
    inns.foreach(print(_))

    val personaF = sparkSession.sparkContext.textFile("/Users/sev7e0/tools/spark-2.4.0-bin-hadoop2.6/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attr => Person(attr(0), attr(1).trim.toInt))
      .toDF()
    personaF.createOrReplaceTempView("person")

    val personDFTable = sparkSession.sql("select * from person")

    personDFTable.show()

    //两种方式获取dataframe中的相关属性
    personDFTable.map(person =>"name:" +person(0)).show()

    personDFTable.map(person =>"name:" +person.getAs[String]("name")).show()

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val maps = personDFTable.map(person=>person.getValuesMap[Any](List("name","age"))).collect()
    maps.foreach(map=>map.foreach(m=>println("name: "+m._1+"----age :"+m._2)))

  }

}
