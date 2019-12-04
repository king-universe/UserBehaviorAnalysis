package com.flink.LoginFailDetect.analysis

import com.flink.LoginFailDetect.model.LoginEvent
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

import org.apache.flink.cep.scala.pattern.Pattern
import scala.collection.Map

/**
  * @author 王犇
  * @date 2019/12/4 17:38
  * @version 1.0
  */
object LoginFailWithCep {



  def main(args: Array[String]): Unit = {
    val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val loginEventStream = see.fromCollection(List(LoginEvent(1, "192.168.8.45", "fail", 1558430842),
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

    val loginPattern = Pattern.begin[LoginEvent]("begin").where(_.loginStatus == "fail").next("next").where(_.loginStatus == "fail").within(Time.seconds(2))
    // 在数据流中匹配出定义好的模式
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginPattern)


    // .select方法传入一个 pattern select function，当检测到定义好的模式序列时就会调用

    val loginFailDataStream = patternStream.select((pattern:Map[String,Iterable[LoginEvent]])=>{
      val first = pattern.getOrElse("begin", null).iterator.next()
      val second = pattern.getOrElse("next", null).iterator.next()
      (second.userId, second.ip, second.loginStatus,second.eventTime)
    })
    // 将匹配到的符合条件的事件打印出来
    loginFailDataStream.print()
    see.execute("Login Fail Detect Job")

  }
}
