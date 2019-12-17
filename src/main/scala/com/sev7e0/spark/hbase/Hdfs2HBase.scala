package com.sev7e0.spark.hbase

import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.spark.{SparkConf, SparkContext}

object Hdfs2HBase {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("spark://spark01:7077")
      .setAppName(Hdfs2HBase.getClass.getName)
      .set("spark.jars", "target/spark-learn-1.0-SNAPSHOT.jar")

    val sparkContext = new SparkContext(conf)

    val userRDD = sparkContext.textFile("hdfs://spark01:9000/spark/users.dat",2).map(_.split("::"))

    userRDD.foreachPartition(iter =>{
      val configuration = HBaseConfiguration.create()
//      configuration.set("hbase.zookeeper.quorum","spark01:2181,spark02:2181,spark03:2181")
      configuration.set("hbase.zookeeper.quorum", "spark01")
      configuration.set("hbase.zookeeper.property.clientPort", "2181")
      //创建连接
      val connection = ConnectionFactory.createConnection(configuration)
      //get table object
      val person = connection.getTable(TableName.valueOf("users"))

      iter.foreach(p=>{
        val arrayList = new util.ArrayList[Put]()
        val put = new Put(p(0).getBytes)
        arrayList.add(put.addColumn("f1".getBytes,"gender".getBytes,p(1).getBytes))
        arrayList.add(put.addColumn("f1".getBytes,"age".getBytes,p(2).getBytes))
        arrayList.add(put.addColumn("f2".getBytes,"position".getBytes,p(3).getBytes))
        arrayList.add(put.addColumn("f2".getBytes,"code".getBytes,p(4).getBytes))
        person.put(arrayList)
      })
    })
    sparkContext.stop()

  }

}
