package com.sev7e0.spark.structured_streaming

import org.apache.spark.sql.SparkSession

object A_2_JoinOperations {

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
    streamDF.join(staticDF, "type")

    var seq = Seq[String]()
    seq = seq :+ "type"
    streamDF.join(staticDF, seq, "right_outer")

    /**
      * 流数据和流态数据进行join操作
      */
    val streamDF1 = session.readStream.load()
    val streamDF2 = session.readStream.load()

    // 默认join使用内连接方式
    // 在事件事件上使用水印
    val impressionWithWaterMarker = streamDF1.withWatermark("impressionTime", "2 hours")
    val clickWihtWaterMarker = streamDF2.withWatermark("clickTime", "3 hours")

    /**
      * 使用内连接时的注意事项:
      * 1. 在两个输入上定义水印延迟，以便引擎知道输入延迟的程度(类似于流聚合)
      * 2. 定义一个跨两个输入的事件时间约束，这样引擎就可以知道当一个输入的旧行与另一个输入匹配时不需要(即不满足时间约束)。这个约束可以用两种方法中的一种来定义。
      * Time range join conditions (e.g. ...JOIN ON leftTime BETWEEN rightTime AND rightTime + INTERVAL 1 HOUR),
      * Join on event-time windows (e.g. ...JOIN ON leftTimeWindow = rightTimeWindow).
      */
    import org.apache.spark.sql.functions.expr
    impressionWithWaterMarker.join(clickWihtWaterMarker, expr(
      """   clickAdId = impressionAdId AND
                                                                   clickTime >= impressionTime AND
                                                                   clickTime <= impressionTime + interval 1 hour"""))

    /**
      * 对于外链接,是一定要指定waterMarker和event-time,这是为了防止产生Null结果,流处理引擎必须知道输入流在何时不再需要去匹配.
      */
    impressionWithWaterMarker.join(clickWihtWaterMarker, expr(
      """   clickAdId = impressionAdId AND
                                                                   clickTime >= impressionTime AND
                                                                   clickTime <= impressionTime + interval 1 hour"""),
      //指定join类型
      joinType = "leftOuter")

    /**
      * Caveats:
      * 外部空结果将生成一个延迟，该延迟取决于指定的水印延迟和时间范围条件。这是因为引擎必须等待那么长的时间来确保没有匹配，
      * 并且将来也不会有更多匹配。
      * 在微批处理引擎的当前实现中，在微批处理的最后会进行水印的高级处理，下一个微批处理使用更新后的水印来清理状态并输出外部结果。
      * 因为我们只在需要处理新数据时触发微批处理，所以如果流中没有接收到新数据，外部结果的生成可能会延迟。简而言之，
      * 如果正在连接的两个输入流中的任何一个在一段时间内不接收数据，外部(两种情况，左或右)输出可能会延迟。
      */
    /**
      * notes:
      *   1.join操作可以串联,df1.join(df2).join(df3)....
      *   2.截止2.3只能在query中使用append output模式中使用join,不支持其他
      *   3.截止2.3之前有一些操作不能在join操作之前使用
      * Cannot use streaming aggregations before joins.
      * Cannot use mapGroupsWithState and flatMapGroupsWithState in Update mode before joins.
      */

  }
}
