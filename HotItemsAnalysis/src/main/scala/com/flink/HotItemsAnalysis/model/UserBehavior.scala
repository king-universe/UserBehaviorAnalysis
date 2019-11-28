package com.flink.HotItemsAnalysis.model

/**
  * @author 王犇
  * @date 2019/11/27 16:54
  * @version 1.0
  */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Long, behavior: String, timeStamp: Long)
