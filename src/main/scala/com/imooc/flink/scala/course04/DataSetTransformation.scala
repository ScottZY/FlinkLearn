package com.imooc.flink.scala.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * Flink Transformation 操作
 */
object DataSetTransformation {
  def main(args: Array[String]): Unit = {
//    创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
//    mapFunction(env)
    filterFunction(env)
  }

  /**
   * filter Function 过滤操作
   * @param env
   */
  def filterFunction(env: ExecutionEnvironment): Unit ={
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.filter(_ > 5).print()
  }


  /**
   * map function : 作用在数据集中的每一个元素之上
   * @param env
   */
  def mapFunction(env: ExecutionEnvironment): Unit ={
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
//    对data中的每个元素都做一个 +1 的操作
//    data.map((x: Int) => x+1).print()
//    data.map((x) => x+1).print()
//    data.map(x => x+1).print()
    data.map(_ + 1).print()

  }

}
