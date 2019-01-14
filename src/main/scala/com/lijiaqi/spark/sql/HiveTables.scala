package com.lijiaqi.spark.sql

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-15 01:06
  **/
object HiveTables {


  case class Record(key: Int, value: String)

  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local")
      .appName("hive table")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    import sparkSession.implicits._
    import sparkSession.sql

    sql("CREATE TABLE  IF NOT EXISTS src (key INT, value STRING) using hive")
    sql("LOAD DATA LOCAL INPATH 'src/main/resources/sparkresource/kv1.txt' into table src")


    sql("select * from src").show()
    sql("select COUNT(*) FROM src").show()

  }

}
