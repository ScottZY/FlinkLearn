package com.imooc.flink.scala.course04

import org.apache.flink.api.common.operators.Order
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
//    mapPartitionFunction(env)
//    firstFunction(env)
    flatMapFunction(env)
  }


  /**
   * FlatMap function：将一个元素变为多个
   * @param env
   */
  def flatMapFunction(env: ExecutionEnvironment): Unit ={
    val info = new ListBuffer[String]()
    info.append("hadoop,spark")
    info.append("hadoop,flink")
    info.append("flink,flink")
    val data = env.fromCollection(info)
//    data.map(_.split(",")).print()  // [Ljava.lang.String;@4b6ac111 使用map和split后无法直接打印内容 打印出来的是一个个对象
    data.flatMap(_.split(",")).print() // hadoop ...
//    词频统计
    data.flatMap(_.split(",")).map((_, 1)).groupBy(0).sum(1).print()
  }
  /**
   * first 函数
   * @param env
   */
  def firstFunction(env: ExecutionEnvironment): Unit ={
    val info = new ListBuffer[(Int, String)]()
    info.append((1, "hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((2, "Ruby"))
    info.append((3, "Linux"))
    info.append((3, "Unix"))
    val data = env.fromCollection(info)
//    按顺序取出前三条
//    data.first(3).print()
//    取出分组后的每个组内的前两条数据
//    data.groupBy(0).first(2).print()
//    分组后根据第二个元素排序(升序)，然后取出前两条
    println("升序排列：")
    data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
    println("降序排列：")
    data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print()

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
