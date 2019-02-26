package com.lijiaqi.spark.structured_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

object StreamingQuery {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("StreamingQuery")
      .getOrCreate()
    val streamDF = session.readStream.load()

    val query = streamDF.writeStream
      .format("console")
      .start()

    /**
      * query manage
      */
    query.id  //唯一id,该id在检查点重新启动时仍然存在

    query.runId //运行中的查询唯一运行id,在start和restart时生成

    query.name  //query名字,可以自动生成也可以指定

    query.stop() //停止命令

    query.exception //如果查询已被错误终止，则为异常

    query.lastProgress //查询的最新进度

    query.recentProgress //查询进度的集合,包含最近的进度

    query.awaitTermination() //阻塞，直到使用stop()或错误终止查询

    query.explain() //打印出查询中的具体细节
    /**
      * note:
      *   你可以通过一个spark session启动多个查询任务,他们将同时运行并共享集群资源,可以使用SparkSession.streams()进行查询任务管理
      */
    session.streams.active
    session.streams.get("id")
    session.streams.awaitAnyTermination()


    /**
      * Monitoring Streaming Query
      */

    //通过附加StreamingQueryListener来异步监视与SparkSession关联的所有查询
    session.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        print(event.id,event.name,event.runId)
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        print(event.progress)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        print(event.exception,event.id,event.runId)
      }
    })

    //spark支持使用Dropwizard进行监控,但需要手动配置启用,启动后将会向配置的监视器发送状态数据
    session.conf.set("spark.sql.streaming.metricsEnabled", "true")
    session.sql("SET spark.sql.streaming.metricsEnabled=true")

    //spark支持使用CheckPoint进行失败的任务恢复,不过需要在query启动前配置checkpointLocation,是一个hdfs的分布式文件系统目录,用于保存进度信息,以便restart时恢复
    streamDF.writeStream
      .format("memory")
      .outputMode("complete")
      .option("checkpointLocation","/dir")
      .start()

  }

}
