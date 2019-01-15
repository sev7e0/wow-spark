package com.lijiaqi.spark.sql

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

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

    val keyDF = sql("select key, value from src where key < 10 order by key")

    val mapDF = keyDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    mapDF.show()

    //可以将数据帧和hive中的数据进行join
    val recordDF = sparkSession.createDataset((1 to 100).map(i => Record(i,s"val_$i")))
    recordDF.createOrReplaceTempView("records")

    sparkSession.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()



    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
    // Save DataFrame to the Hive managed table
    val df = sparkSession.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()
  }

}
