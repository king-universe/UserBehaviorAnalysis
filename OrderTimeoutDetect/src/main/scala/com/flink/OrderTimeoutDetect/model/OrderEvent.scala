package com.flink.OrderTimeoutDetect.model

/**
  * @author 王犇
  * @date 2019/12/4 19:31
  * @version 1.0
  */
case class OrderEvent(orderId: Long, payStatue: String, eventTime: Long)
