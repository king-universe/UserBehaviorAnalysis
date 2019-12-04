package com.flink.OrderTimeoutDetect.analysis

import com.flink.OrderTimeoutDetect.model.{OrderEvent, OrderResult}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author 王犇
  * @date 2019/12/4 19:30
  * @version 1.0
  */
object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val orderEventStream = see.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime)

    val pattern = Pattern.begin[OrderEvent]("create").where(_.payStatue == "create").next("next").where(_.payStatue == "pay").within(Time.minutes(15))

    // 定义一个输出标签
    val orderTimeoutOutput = OutputTag[OrderEvent]("orderTimeout")

    val patternStream = CEP.pattern(orderEventStream.keyBy(_.orderId), pattern)



  val complexResult = patternStream.select(orderTimeoutOutput) {
    // 对于已超时的部分模式匹配的事件序列，会调用这个函数
    (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
      val createOrder = pattern.get("begin")
      OrderResult(createOrder.get.iterator.next().orderId, "timeout")
    }
  } {
    // 检测到定义好的模式序列时，就会调用这个函数
    pattern: Map[String, Iterable[OrderEvent]] => {
      val payOrder = pattern.get("next")
      OrderResult(payOrder.get.iterator.next().orderId, "success")
    }
  }


  }
}
