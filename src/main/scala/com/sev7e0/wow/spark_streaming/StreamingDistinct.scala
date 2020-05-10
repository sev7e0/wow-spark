package com.sev7e0.wow.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * spark streaming 实现Distinct
 */
object StreamingDistinct {
  
  def main(args: Array[String]): Unit = {
  
    val nameConf = new SparkConf().setMaster("local[*]").setAppName(StreamingDistinct.getClass.getName)
  
    val context = new StreamingContext(nameConf, Seconds(2))
    val l = System.currentTimeMillis()
    context.checkpoint("target/checkpoint/"+l+"/")
  
    val scoketDS = context.socketTextStream("localhost", 9999, storageLevel = StorageLevel.MEMORY_ONLY)
  
    val wordsDS = scoketDS.flatMap(line => line.split(" "))
  
    val mapDS = wordsDS.map((_, 1))
  
    //去重的话需要考虑状态。
    val value1 = mapDS.updateStateByKey((value: Seq[Int], state: Option[Int]) => {
      var s = state.getOrElse(0)
      for (_ <- value) {
        if (s == 0){
          s += 1
        }
      }
      Option(s)
    }).map(key=>key._1)
    
    value1.print()
    println(value1.count())
    context.start()
    context.awaitTermination()
  }
  
}
