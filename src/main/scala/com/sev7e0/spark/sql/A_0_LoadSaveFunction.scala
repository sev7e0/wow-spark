package com.sev7e0.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 1.可以读取指定格式文件，同时生成指定格式的文件 目前支持 json，parquet，orc，hivetable，jdbc，avro file
  * 2.可以直接在文件中执行SQL语句
  * 3.支持一下几种保存模式，ErrorIfExists；Append；Overwrite；Ignore
  * 4.支持将DataFrame保存到持久表（不同于createOrReplaceTempView,saveAsTable将会持久化到存储，即使spark程序重启也会存在）
  * 5.在spark2.1中持久化的原数据信息将会保存到Hive元数据中，好处是，在查询时可以减少对不必要的partition进行查询，配置单元DDL，如改变表分区…集合位置现在可用于使用数据源API创建的表。
  * 6.对基于文件的数据源，也可以进行bucket存储，排序和分区，bucket和排序只适用于持久表
  *
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-08 01:01
  **/
object A_0_LoadSaveFunction {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local")
      .appName("LoadSaveFunction")
      .getOrCreate()

    val users = session.read.load("/Users/sev7e0/workspace/idea-workspace/sparklearn/src/main/resources/sparkresource/users.parquet")
    users.select("name","favorite_color").write.mode(saveMode = "overwrite").save("namesAndFavColors.parquet")

    /**
      * load中的路径可以使用文件夹，但是只能存在一种类型，就是format中指定的类型
      */
    val orcDF = session.read.format("orc").load("/Users/sev7e0/workspace/idea-workspace/sparklearn/src/main/resources/spark resource")
    orcDF.show()

//    session.sqlContext
//    users.write.format("orc")
//      .option("orc.bloom.filter.columns","favorite_color")
//      .option("orc.dictionary.key.threshold","1.0")
//      .save("users_with_option.orc")

    //k可以指定格式名称，从任意数据源进行转换
    val peopleDS = session.read.json("src/main/resources/sparkresource/people.json")
    peopleDS.select("name","age").write.mode(saveMode = "overwrite").format("parquet").save("nameAndAge.parquet")

    //从指定格式转换为另一种格式
    val dataFrameCSV = session.read.option("sep",";").option("inferSchema","true")
      .option("header","true").csv("src/main/resources/sparkresource/people.csv")
    dataFrameCSV.select("name","age").write.mode(saveMode = "overwrite").format("json").save("csvToJson.json")

    //直接在文件中执行SQL
    val fromParquetFile = session.sql("SELECT * FROM parquet.`src/main/resources/sparkresource/users.parquet`")
//    fromParquetFile.show()

    //对于生成指定文件可以指定key进行分区
    users.write.partitionBy("favorite_color").format("parquet").mode(saveMode = "overwrite")
      .save("namesPartitionByColor.parquet")
  }

}
