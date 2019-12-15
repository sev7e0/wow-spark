package com.sev7e0.spark.core.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UV {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UV-test").setMaster("spark://spark01:7077")
    sparkConf.set("spark.jars", "target/spark-learn-1.0-SNAPSHOT.jar")
    //2、构建SparkContext
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、读取数据文件
    val data: RDD[String] = sc.textFile("hdfs://spark01:9000/spark/access.log")


    //4、切分每一行，获取第一个元素 也就是ip
    val ips: RDD[String] = data.map( x =>x.split(" ")(0))

    //5、按照ip去重
    val distinctRDD: RDD[String] = ips.distinct()

    //6、统计uv
    val uv: Long = distinctRDD.count()
    println("UV:"+uv)


    sc.stop()

  }
}

