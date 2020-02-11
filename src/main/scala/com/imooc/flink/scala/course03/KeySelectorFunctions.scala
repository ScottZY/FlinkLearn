package com.imooc.flink.scala.course03

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Key Selector Functions(key 选择器)
 */
object KeySelectorFunctions {
  def main(args: Array[String]): Unit = {
    //    创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    //    隐式转换
    import org.apache.flink.api.scala._
    //    transform
    text.flatMap(_.toLowerCase.split(","))
      .map(x => WC(x, 1))
      .keyBy(x => x.word)
      .sum("count")
      .print().setParallelism(1)
    //    执行
    env.execute("KeySelectorFunctions")
  }

  case class WC(word: String, count: Int);
}
