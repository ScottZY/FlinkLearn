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
//    flatMapFunction(env)
//    distinctFunction(env)
//    joinFunction(env)
    outJoinFunction(env)
  }


  /**
   * 外连接
   * @param env
   */
  def outJoinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = new ListBuffer[(Int, String)]()
    info1.append((1, "Montreal"))
    info1.append((2, "Toronto"))
    info1.append((3, "Shanghai"))
    info1.append((4, "York"))
    info1.append((6, "Beijing"))

    val info2 = new ListBuffer[(Int, String)]()
    info2.append((1, "James"))
    info2.append((2, "Will"))
    info2.append((3, "Reymond"))
    info2.append((4, "Jack"))
    info2.append((5, "XiaoMing"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

//    左外连接  以左边数据位基准 如果右边的数据中没有与之对应的 那么左边对应的数据就为空 但是左边的数据时全部显示的
    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((left, right) => {
      if (right == null){
        (left._1, "无", left._2)
      }else{
        (left._1, right._2, left._2)
      }
    }).print()
//    (3,Reymond,Shanghai)
//    (1,James,Montreal)
//    (6,无,Beijing)
//    (2,Will,Toronto)
//    (4,Jack,York)

    //    右外连接  以右边数据位基准 如果左边的数据中没有与之对应的 那么右边对应的数据就为空 但是右边的数据时全部显示的
    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((left, right) => {
      if (left == null){
        (right._1, right._2, "无")
      }else{
        (left._1, right._2, left._2)
      }
    }).print()
//    (3,Reymond,Shanghai)
//    (1,James,Montreal)
//    (5,XiaoMing,无)
//    (2,Will,Toronto)
//    (4,Jack,York)

//    全连接
    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((left, right) => {
      if (left == null){
        (right._1, right._2, "无")
      }else if(right == null){
        (left._1, "无", left._2)
      } else{
        (left._1, right._2, left._2)
      }
    }).print()
//    (3,Reymond,Shanghai)
//    (1,James,Montreal)
//    (5,XiaoMing,无)
//    (6,无,Beijing)
//    (2,Will,Toronto)
//    (4,Jack,York)


  }


  /**
   * join function  内连接
   * @param env
   */
  def joinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = new ListBuffer[(Int, String)]()
    info1.append((1, "Montreal"))
    info1.append((2, "Toronto"))
    info1.append((3, "Shanghai"))
    info1.append((4, "York"))
    info1.append((6, "Beijing"))

    val info2 = new ListBuffer[(Int, String)]()
    info2.append((1, "James"))
    info2.append((2, "Will"))
    info2.append((3, "Reymond"))
    info2.append((4, "Jack"))
    info2.append((5, "XiaoMing"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

//    执行join操作 data1和data2相join  条件是 data1(左边的) 中的第0元素个 和 data2(右边的) 中的第0元素个相等
//    apply：创建一个新的[[DataSet]]，其中每对连接的元素的结果都是给定函数的结果。
    data1.join(data2).where(0).equalTo(0).apply((left, right) => {
      (left._1, right._2, left._2)
    }).print()

//    (3,Reymond,Shanghai)
//    (1,James,Montreal)
//    (2,Will,Toronto)
//    (4,Jack,York)


  }



  /**
   * distinct function
    * @param env
   */
  def distinctFunction(env: ExecutionEnvironment): Unit ={
    val list = new ListBuffer[String]()
    list.append("hadoop,hadoop")
    list.append("hadoop,spark")
    list.append("hadoop,flink")
    val data = env.fromCollection(list)
    data.flatMap(_.split(",")).distinct().print()
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
