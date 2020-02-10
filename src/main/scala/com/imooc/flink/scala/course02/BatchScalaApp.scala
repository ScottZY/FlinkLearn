package com.imooc.flink.java.course02

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * 使用scala开发flink批处理应用程序(wordcount)
 */
object BatchScalaApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input = "file:\\D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\hello.txt"
    val text = env.readTextFile(input)
    text.print()
//    引入Scala隐式转换 --- 否则代码后边会有 (...) 报错
    import org.apache.flink.api.scala._
    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0).sum(1)
      .print()
  }
}
