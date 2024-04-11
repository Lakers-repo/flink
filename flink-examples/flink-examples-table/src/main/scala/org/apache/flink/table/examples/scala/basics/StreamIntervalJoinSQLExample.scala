package org.apache.flink.table.examples.scala.basics

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{FieldExpression, Schema, TableEnvironment, UnresolvedFieldExpression}
import org.apache.flink.table.api.bridge.scala.{dataStreamConversions, tableConversions, StreamTableEnvironment}
import org.apache.flink.types.Row

import java.sql.Timestamp
import java.time.ZoneId

import scala.collection.mutable

object StreamIntervalJoinSQLExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
//    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // order data
    val ordersData = new mutable.MutableList[(String, String, Timestamp)]
    ordersData.+=(("001", "iphone", new Timestamp(1545800002000L)))
    ordersData.+=(("002", "mac", new Timestamp(1545800003000L)))
    ordersData.+=(("003", "book", new Timestamp(1545800004000L)))
    ordersData.+=(("004", "cup", new Timestamp(1545800018000L)))

    // payment data
    val paymentData = new mutable.MutableList[(String, String, Timestamp)]
    paymentData.+=(("001", "alipay", new Timestamp(1545803501000L)))
    paymentData.+=(("002", "card", new Timestamp(1545803602000L)))
    paymentData.+=(("003", "card", new Timestamp(1545803610000L)))
    paymentData.+=(("004", "alipay", new Timestamp(1545803611000L)))

    // schema
    val orders = env
      .fromCollection(ordersData)
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .toTable(tEnv, $"orderId", $"productName", $"orderTime")

    val ratesHistory = env
      .fromCollection(paymentData)
      .assignTimestampsAndWatermarks(new TimestampExtractor())
      .toTable(tEnv, $"orderId", $"payType", $"payTime")

    tEnv.createTemporaryView("Orders", orders)
    tEnv.createTemporaryView("Payment", ratesHistory)

    var sqlQuery =
      """
        |SELECT
        |  o.orderId,
        |  o.productName,
        |  p.payType,
        |  o.orderTime,
        |  cast(payTime as timestamp) as payTime
        |FROM
        |  Orders AS o JOIN Payment AS p ON o.orderId = p.orderId AND
        | p.payTime BETWEEN orderTime AND orderTime + INTERVAL '1' HOUR
        |""".stripMargin
//    tEnv.explainSql(sqlQuery)
    tEnv.executeSql(sqlQuery).print()
//    tEnv.createTemporaryView("TemporalJoinResult", tEnv.sqlQuery(sqlQuery))

//    val result = tEnv.from("TemporalJoinResult").toChangelogStream
//    result.print()
    env.execute()
  }
}

class TimestampExtractor
  extends BoundedOutOfOrdernessTimestampExtractor[(String, String, Timestamp)](Time.seconds(10)) {
  override def extractTimestamp(element: (String, String, Timestamp)): Long = {
    element._3.getTime
  }
}
