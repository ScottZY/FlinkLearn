package com.imooc.flink.scala.course04

import scala.util.Random

/**
 * ubuntu 中测试提交
 */
object DBUtils {
  def getConnection() ={
    new Random().nextInt(10)+"1"
  }
  def returnFunction(connection: String): Unit ={

  }

}
