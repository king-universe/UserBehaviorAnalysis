package com.flink.LoginFailDetect.analysis

import com.flink.LoginFailDetect.model.LoginEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @author 王犇
  * @date 2019/12/4 17:04
  * @version 1.0
  */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    see.fromCollection(List(LoginEvent(1, "192.168.8.45", "fail", 1558430842),
      LoginEvent(1, "192.168.8.46", "fail", 1558430843),
      LoginEvent(1, "192.168.8.47", "fail", 1558430844),
      LoginEvent(1, "192.168.8.48", "fail", 1558430845),
      LoginEvent(1, "192.168.8.49", "fail", 1558430846),
      LoginEvent(1, "192.168.8.49", "fail", 1558430848),
      LoginEvent(1, "192.168.8.49", "fail", 1558430844),
      LoginEvent(1, "192.168.8.49", "fail", 1558430852),
      LoginEvent(1, "192.168.8.49", "fail", 1558430856),
      LoginEvent(1, "192.168.8.49", "fail", 1558430858),
      LoginEvent(1, "192.168.8.50", "success", 1558430847)))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)
      .process(new MatchFuction())
      .print()
    see.execute()
  }

  class MatchFuction extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {

    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved login", classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
      if (value.loginStatus == "fail") {
        loginState.add(value)
      }
      // 注册定时器，触发事件设定为2秒后
      ctx.timerService().registerEventTimeTimer(value.eventTime + 2 * 1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
      super.onTimer(timestamp, ctx, out)
      val allLogins: ListBuffer[LoginEvent] = ListBuffer()

      import scala.collection.JavaConversions._
      for (item <- loginState.get) {
        allLogins += item
      }
      loginState.clear()

      if (allLogins.length > 1) {
        out.collect(allLogins.last)
      }
    }
  }

}
