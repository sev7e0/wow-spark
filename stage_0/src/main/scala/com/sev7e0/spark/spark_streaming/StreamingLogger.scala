package com.sev7e0.spark.spark_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

/**
  * @program: spark-learn
  * @description:
  * @author: Lijiaqi
  * @create: 2019-03-12 17:12
  **/
object StreamingLogger extends Logging {

  def setLoggerLevel(): Unit = {
    val elements = Logger.getRootLogger.getAllAppenders.hasMoreElements

    if (!elements) {
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

}
