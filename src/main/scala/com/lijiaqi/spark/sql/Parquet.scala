package com.lijiaqi.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-01-09 22:49
  **/
object Parquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetTest")
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("src/main/resources/sparkresource/people.json")

    jsonDF.write.format("parquet").mode("overwrite")
      .save("src/main/resources/sparkresource/people.parquet")

    val peopleDf = spark.read.parquet("src/main/resources/sparkresource/people.parquet")

    peopleDf.createOrReplaceTempView("people")
    val namesDF = spark.sql("select name from people where age between 13 and 19")

    namesDF.show()

    /**
      * +------+
      * |  name|
      * +------+
      * |Justin|
      * +------+
      */


    import spark.implicits._
    //parquet能够检测比进行格式合并
    val squareDF = spark.sparkContext.makeRDD(1 to 5).map(a =>(a,a*a)).toDF("value", "square")
    squareDF.write.mode("overwrite").parquet("src/main/resources/sparkresource/parquet/key=1")

    val cubeDF = spark.sparkContext.makeRDD(6 to 10 ).map(a =>(a, a*a*a)).toDF("value", "cube")
    cubeDF.write.mode("overwrite").parquet("src/main/resources/sparkresource/parquet/key=2")

    //默认1.5后关闭自动合并的功能
    val meageDF = spark.read.option("mergeSchema","true").parquet("src/main/resources/sparkresource/parquet")

    meageDF.show()
    meageDF.printSchema()

    /**
      * +-----+------+----+---+
      * |value|square|cube|key|
      * +-----+------+----+---+
      * |    1|     1|null|  1|
      * |    2|     4|null|  1|
      * |    3|     9|null|  1|
      * |    4|    16|null|  1|
      * |    5|    25|null|  1|
      * |    6|  null| 216|  2|
      * |    7|  null| 343|  2|
      * |    8|  null| 512|  2|
      * |    9|  null| 729|  2|
      * |   10|  null|1000|  2|
      * +-----+------+----+---+
      *
      * root
      * |-- value: integer (nullable = true)
      * |-- square: integer (nullable = true)
      * |-- cube: integer (nullable = true)
      * |-- key: integer (nullable = true)
      */

    /**
      * +-----+------+---+
      * |value|square|key|
      * +-----+------+---+
      * |    1|     1|  1|
      * |    2|     4|  1|
      * |    3|     9|  1|
      * |    4|    16|  1|
      * |    5|    25|  1|
      * |    6|  null|  2|
      * |    7|  null|  2|
      * |    8|  null|  2|
      * |    9|  null|  2|
      * |   10|  null|  2|
      * +-----+------+---+
      *
      * root
      * |-- value: integer (nullable = true)
      * |-- square: integer (nullable = true)
      * |-- key: integer (nullable = true)
      * 未开启自动合并功能
      */
  }
}
