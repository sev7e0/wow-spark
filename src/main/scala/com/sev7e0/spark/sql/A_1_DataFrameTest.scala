package com.sev7e0.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @program: spark-learn
  * @description: 创建DataFrame的两种方式，第一种通过reflection获取schema，第二种手动自定schema
  * @author: Lijiaqi
  * @create: 2019-01-02 23:57
  **/

object A_1_DataFrameTest {
  case class Person( name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("DataSetTest")
      .master("local")
      .getOrCreate()
    import sparkSession.implicits._

    /**
      * 第一种方式，通过person对象
      */
    val personaF = sparkSession.sparkContext.textFile("src/main/resources/sparkresource/people.txt")
      .map(_.split(","))
      .map(attr => Person(attr(0), attr(1).trim.toInt))
      .toDF()
    personaF.createOrReplaceTempView("person")

    val personDFTable = sparkSession.sql("select * from person")

    personDFTable.show()

    //两种方式获取dataframe中的相关属性
    personDFTable.map(person =>"name:" +person(0)).show()

    personDFTable.map(person =>"name:" +person.getAs[String]("name")).show()

    //没有为数据集[map[k，v]预先定义的编码器，显式定义
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    val maps = personDFTable.map(person=>person.getValuesMap[Any](List("name","age"))).collect()
    maps.foreach(map=>map.foreach(m=>println("name: "+m._1+"----age :"+m._2)))


    /**
      *第二种方式，指定schema
      * Programmatically Specifying the Schema
      */
      //定义一个schema
    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(filedName => StructField(filedName, StringType, nullable = true))
    val structType = StructType(fields)

    val personRDD = sparkSession.sparkContext.textFile("src/main/resources/sparkresource/people.txt")
      .map(_.split(","))
      //将RDD转换为行
      .map(attr => Row(attr(0), attr(1).trim))
    //将schema应用于RDD，并创建df
    sparkSession.createDataFrame(personRDD,structType).createOrReplaceTempView("people1")
    val dataFrameBySchema = sparkSession.sql("select name,age from people1 where age > 19 ")
    dataFrameBySchema.show()
  }

}
