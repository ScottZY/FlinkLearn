package com.imooc.flink.scala.course04

import scala.util.Random

/**
 * by ubuntu
 */
object DBUtils {
  def getConnection() ={
    new Random().nextInt(10)+"1"
  }
  def returnFunction(connection: String): Unit ={

  }

}
