package com.sev7e0.wow.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * spark streaming 实现UV
 */
object StreamingUV {
  
  def main(args: Array[String]): Unit = {
  
    val nameConf = new SparkConf().setMaster("local[*]").setAppName(StreamingUV.getClass.getName)
  
    val context = new StreamingContext(nameConf, Seconds(2))
  
    val l = System.currentTimeMillis()
    context.checkpoint("target/checkpoint/"+l+"/")
  
    val scoketDS = context.socketTextStream("localhost", 9999, storageLevel = StorageLevel.MEMORY_ONLY)
  
    val wordsDS = scoketDS.flatMap(line => line.split(" "))
  
    val mapDS = wordsDS.map((_, 1))
  
    val value1 = mapDS.updateStateByKey((value: Seq[Int], state: Option[Int]) => {
      var s = state.getOrElse(0)
      for (num <- value) {
        s += num
      }
      Option(s)
    })
    
    value1.print()
    
    context.start()
    context.awaitTermination()
  }
  
}
