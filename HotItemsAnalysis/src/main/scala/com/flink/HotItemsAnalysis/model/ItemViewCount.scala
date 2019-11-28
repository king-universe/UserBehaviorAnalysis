package com.flink.HotItemsAnalysis.model

/**
  * @author 王犇
  * @date 2019/11/28 10:37
  * @version 1.0
  */
case class ItemViewCount (itemId: Long, windowEnd: Long, count: Long)
