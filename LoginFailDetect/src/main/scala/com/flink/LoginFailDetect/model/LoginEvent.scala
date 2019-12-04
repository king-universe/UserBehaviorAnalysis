package com.flink.LoginFailDetect.model

/**
  * @author 王犇
  * @date 2019/12/4 17:06
  * @version 1.0
  */
case class LoginEvent(userId: Long, ip: String, loginStatus: String, eventTime: Long)
