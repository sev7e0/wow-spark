package com.lijiaqi.spark.structured_streaming

import org.apache.spark.sql.SparkSession

object JoinOperations {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName("JoinOperations")
      .master("local")
      .getOrCreate()
    // 获取流数据
    val streamDF = session.readStream.load()

    val staticDF = session.read.load("/")

    /**
      * 流数据和静态数据进行join操作
      */
    // 使用type进行join操作
    streamDF.join(staticDF,"type")

    var seq = Seq[String]()
    seq = seq :+ "type"
    streamDF.join(staticDF, seq, "right_outer")

    /**
      * 流数据和流态数据进行join操作
      */
    val streamDF1 = session.readStream.load()
    val streamDF2 = session.readStream.load()


  }
}
