package com.sev7e0.spark.core.scala

import org.apache.spark._

/**
 * 统计每行出现的次数
 */
object LineCount_Scala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("LineCount_Scala")

    val context = new SparkContext(conf)

    //从hdfs、本地文件系统（在所有节点上都可用）或任何hadoop支持的文件系统uri读取文本文件，并将其作为字符串的RDD返回。 注意路径格式
    val lineRdd = context.textFile("src/main/resources/sparkresource/people.txt")

    //使用关联和交换reduce函数合并每个键的值。这还将在将结果发送到reducer之前在每个映射器上本地执行合并，类似于在mapreduce中的“combiner”。
    // 输出将使用现有的partitioner并行级别进行哈希分区。默认使用HashPartitioner
    val lineCount = lineRdd.map(line => (line, 1)).reduceByKey(_ + _)

    lineCount.foreach(line => println(line._1 + ": " + line._2 + " times"))
  }
}
