package com.imooc.flink.scala.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer
/**
 * Flink Transformation 操作
 */
object DataSetTransformation {
  def main(args: Array[String]): Unit = {
//    创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
//    mapFunction(env)
//    filterFunction(env)
    mapPartitionFunction(env)
  }


  /**
   * map partition function
   * 模拟需求：100个需求插入到数据库中
   * @param env
   */
  def mapPartitionFunction(env: ExecutionEnvironment): Unit ={
    val stu = new ListBuffer[String]
//    模拟100个学生
    for (i <- 1 to 100){
      stu.append("student: "+i)
    }

    val data = env.fromCollection(stu).setParallelism(4)
//    使用map获取到100个连接
//    data.map(x => {
//      val connection = DBUtils.getConnection()
//      println(connection + "....")
////      TODO ... 数据保存到DB
//      DBUtils.returnFunction(connection)
//    }).print()

//  使用mapPartition 1次连接 可设置并行度进行控制
    data.mapPartition(x => {
      val connection = DBUtils.getConnection()
      println(connection + "....")
      //      TODO ... 数据保存到DB
      DBUtils.returnFunction(connection)
      x  // mapPartition需要返回
    }).print()
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
