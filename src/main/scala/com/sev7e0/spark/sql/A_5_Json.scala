package com.sev7e0.spark.sql

import org.apache.spark.sql.SparkSession

/**
  *  1.spark可以直接自动将json转为数据集   sparkSession.read.json("path")
  *  2.必须要保证json文件的格式正确
  */

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-15 00:34
  **/
object A_5_Json {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("json")
      .master("local")
      .getOrCreate()
    import sparkSession.implicits._
    val frame = sparkSession.read.json("src/main/resources/sparkresource/people.json")
    frame.printSchema()

    frame.createOrReplaceTempView("people")

    val NamesDF = sparkSession.sql("select * from people")

    NamesDF.show()

    /**
      * +----+-------+
      * | age|   name|
      * +----+-------+
      * |null|Michael|
      * |  30|   Andy|
      * |  19| Justin|
      * +----+-------+
      */

    val otherPeople = sparkSession.createDataset("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherDF = sparkSession.read.json(otherPeople)
    otherDF.show()

    /**
      * +---------------+----+
      * |        address|name|
      * +---------------+----+
      * |[Columbus,Ohio]| Yin|
      * +---------------+----+
      */

  }

}
