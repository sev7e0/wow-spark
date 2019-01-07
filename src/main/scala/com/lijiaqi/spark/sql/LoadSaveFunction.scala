package com.lijiaqi.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-08 01:01
  **/
object LoadSaveFunction {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local")
      .appName("LoadSaveFunction")
      .getOrCreate()


    val users = session.read.load("src/main/resources/sparkresource/users.parquet")
    users.select("name","favorite_color").write.save("namesAndFavColors.parquet")

    //k可以指定格式名称，从任意数据源进行转换
    val peopleDS = session.read.json("src/main/resources/sparkresource/people.json")
    peopleDS.select("name","age").write.format("parquet").save("nameAndAge.parquet")

  }

}
