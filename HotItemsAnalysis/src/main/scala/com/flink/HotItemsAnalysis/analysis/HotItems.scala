package com.flink.HotItemsAnalysis.analysis

import com.flink.HotItemsAnalysis.model.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author 王犇
  * @date 2019/11/27 16:50
  * @version 1.0
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    val see: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    see.setParallelism(1)


    import org.apache.flink.api.scala._
    //662867,2244074,1575622,pv,1511658000
    see.readTextFile("D:\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(a => {
        val array = a.split(",")
        if (array.size >= 5) {
          UserBehavior(array(0).toLong, array(1).toLong, array(2).toLong, array(3), array(4).toLong)
        } else {
          UserBehavior(0l, 0l, 0l, "", 0l)
        }
      }).assignAscendingTimestamps(_.timeStamp * 1000)
      .filter(_.behavior.equals("pv"))
      .keyBy("itemId")
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .aggregate(new CountAgg, new WindowResultFunction)
      .keyBy("windowEnd")
      .print()

    see.execute()
  }

  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

}
