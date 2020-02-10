package com.imooc.flink.scala.course02

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * 使用Scala 开发flink流处理应用
 */
object StreamingWCScalaApp {
  def main(args: Array[String]): Unit = {
//    创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("192.168.50.205", 9999)
//    隐式转换
    import org.apache.flink.api.scala._
//    transform
    text.flatMap(_.toLowerCase.split(","))
      .map(x => WC(x, 1))
      .keyBy("word")
      .sum("count")
      .print().setParallelism(1)
//    执行
    env.execute("StreamingWCScalaApp")
  }

  case class WC(word: String, count: Int);
}
