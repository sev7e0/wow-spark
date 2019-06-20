package com.sev7e0.spark.structured_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener

object A_4_StreamingQuery {
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
    query.id //唯一id,该id在检查点重新启动时仍然存在

    query.runId //运行中的查询唯一运行id,在start和restart时生成

    query.name //query名字,可以自动生成也可以指定

    query.stop() //停止命令

    query.exception //如果查询已被错误终止，则为异常

    query.lastProgress //查询的最新进度

    query.recentProgress //查询进度的集合,包含最近的进度

    query.awaitTermination() //阻塞，直到使用stop()或错误终止查询

    query.explain() //打印出查询中的具体细节
    /**
      * note:
      * 你可以通过一个spark session启动多个查询任务,他们将同时运行并共享集群资源,可以使用SparkSession.streams()进行查询任务管理
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
        print(event.id, event.name, event.runId)
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        print(event.progress)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        print(event.exception, event.id, event.runId)
      }
    })

    //spark支持使用Dropwizard进行监控,但需要手动配置启用,启动后将会向配置的监视器发送状态数据
    session.conf.set("spark.sql.streaming.metricsEnabled", "true")
    session.sql("SET spark.sql.streaming.metricsEnabled=true")

    //spark支持使用CheckPoint进行失败的任务恢复,不过需要在query启动前配置checkpointLocation,是一个hdfs的分布式文件系统目录,用于保存进度信息,以便restart时恢复
    streamDF.writeStream
      .format("memory")
      .outputMode("complete")
      .option("checkpointLocation", "/dir")
      .start()

    /**
      * Recovery Semantics after Changes in a Streaming Query
      * There are limitations on what changes in a streaming query are allowed between restarts from the same checkpoint location. Here are a few kinds of changes that are either not allowed, or the effect of the change is not well-defined. For all of them:
      *
      * The term allowed means you can do the specified change but whether the semantics of its effect is well-defined depends on the query and the change.
      *
      * The term not allowed means you should not do the specified change as the restarted query is likely to fail with unpredictable errors. sdf represents a streaming DataFrame/Dataset generated with sparkSession.readStream.
      *
      * Types of changes
      *
      * Changes in the number or type (i.e. different source) of input sources: This is not allowed.
      *
      * Changes in the parameters of input sources: Whether this is allowed and whether the semantics of the change are well-defined depends on the source and the query. Here are a few examples.
      *
      * Addition/deletion/modification of rate limits is allowed: spark.readStream.format("kafka").option("subscribe", "topic") to spark.readStream.format("kafka").option("subscribe", "topic").option("maxOffsetsPerTrigger", ...)
      *
      * Changes to subscribed topics/files is generally not allowed as the results are unpredictable: spark.readStream.format("kafka").option("subscribe", "topic") to spark.readStream.format("kafka").option("subscribe", "newTopic")
      *
      * Changes in the type of output sink: Changes between a few specific combinations of sinks are allowed. This needs to be verified on a case-by-case basis. Here are a few examples.
      *
      * File sink to Kafka sink is allowed. Kafka will see only the new data.
      *
      * Kafka sink to file sink is not allowed.
      *
      * Kafka sink changed to foreach, or vice versa is allowed.
      *
      * Changes in the parameters of output sink: Whether this is allowed and whether the semantics of the change are well-defined depends on the sink and the query. Here are a few examples.
      *
      * Changes to output directory of a file sink is not allowed: sdf.writeStream.format("parquet").option("path", "/somePath") to sdf.writeStream.format("parquet").option("path", "/anotherPath")
      *
      * Changes to output topic is allowed: sdf.writeStream.format("kafka").option("topic", "someTopic") to sdf.writeStream.format("kafka").option("topic", "anotherTopic")
      *
      * Changes to the user-defined foreach sink (that is, the ForeachWriter code) is allowed, but the semantics of the change depends on the code.
      *
      * *Changes in projection / filter / map-like operations**: Some cases are allowed. For example:
      *
      * Addition / deletion of filters is allowed: sdf.selectExpr("a") to sdf.where(...).selectExpr("a").filter(...).
      *
      * Changes in projections with same output schema is allowed: sdf.selectExpr("stringColumn AS json").writeStream to sdf.selectExpr("anotherStringColumn AS json").writeStream
      *
      * Changes in projections with different output schema are conditionally allowed: sdf.selectExpr("a").writeStream to sdf.selectExpr("b").writeStream is allowed only if the output sink allows the schema change from "a" to "b".
      *
      * Changes in stateful operations: Some operations in streaming queries need to maintain state data in order to continuously update the result. Structured Streaming automatically checkpoints the state data to fault-tolerant storage (for example, HDFS, AWS S3, Azure Blob storage) and restores it after restart. However, this assumes that the schema of the state data remains same across restarts. This means that any changes (that is, additions, deletions, or schema modifications) to the stateful operations of a streaming query are not allowed between restarts. Here is the list of stateful operations whose schema should not be changed between restarts in order to ensure state recovery:
      *
      * Streaming aggregation: For example, sdf.groupBy("a").agg(...). Any change in number or type of grouping keys or aggregates is not allowed.
      *
      * Streaming deduplication: For example, sdf.dropDuplicates("a"). Any change in number or type of grouping keys or aggregates is not allowed.
      *
      * Stream-stream join: For example, sdf1.join(sdf2, ...) (i.e. both inputs are generated with sparkSession.readStream). Changes in the schema or equi-joining columns are not allowed. Changes in join type (outer or inner) not allowed. Other changes in the join condition are ill-defined.
      *
      * Arbitrary stateful operation: For example, sdf.groupByKey(...).mapGroupsWithState(...) or sdf.groupByKey(...).flatMapGroupsWithState(...). Any change to the schema of the user-defined state and the type of timeout is not allowed. Any change within the user-defined state-mapping function are allowed, but the semantic effect of the change depends on the user-defined logic. If you really want to support state schema changes, then you can explicitly encode/decode your complex state data structures into bytes using an encoding/decoding scheme that supports schema migration. For example, if you save your state as Avro-encoded bytes, then you are free to change the Avro-state-schema between query restarts as the binary state will always be restored successfully.
      */

  }

}
