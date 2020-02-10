package com.imooc.flink.scala.course04

import com.imooc.flink.Course04.Persion
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * Scala DataSet API编程
 */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
//    创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
//    fromCollection(env) // 读取集合
//    textFile(env) // 读取文件或文件夹
//    csvFile(env) // 读取csv文件
//    readRecursiveFile(env)
    readCompressionFile(env)
  }


  /**
   * 读取压缩文件
   * @param env
   */
  def readCompressionFile(env: ExecutionEnvironment): Unit ={
    val filepath = "D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\inputs.tgz"
    env.readTextFile(filepath).print()

  }


  /**
   * 从递归文件夹中读取数据
   * @param env
   */
  def readRecursiveFile(env: ExecutionEnvironment): Unit ={
//    错误的读法
    val filepath = "D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\nested"
    env.readTextFile(filepath).print()

    println("~~~~~~~~~~~~~我是分割线~~~~~~~~~~~~~~~~~~~~~~~")

//    正确的读法
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)
    env.readTextFile(filepath).withParameters(parameters).print()

  }


  /**
   * 读取csv文件
   * @param env
   */
  def csvFile(env: ExecutionEnvironment): Unit ={
    val csvPath = "D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\people.csv"
//    读取csv文件时需要指定每一列的类型  ignoreFirstLine = true忽略第一行
//    env.readCsvFile[(String, Int, String)](csvPath, ignoreFirstLine = true).print()
//    只读取前两列  includedFields = Array(0, 2)可以不写， 只要在readCsvFile中指定要读取列的数据类型也可以，默认从头开始
//    env.readCsvFile[(String, Int)](csvPath, ignoreFirstLine = true, includedFields = Array(0, 2))
//    传入一个类的方式
    case class MyCaseClass(name: String, age: Int)
//    env.readCsvFile[MyCaseClass](csvPath, ignoreFirstLine = true, includedFields = Array(0, 2)).print()
//    MyCaseClass(Bob,32)
//    MyCaseClass(Jorge,30)
//    读取 java pojo
    env.readCsvFile[Persion](csvPath, ignoreFirstLine = true, pojoFields = Array("name", "age", "work")).print()
//    Persion{name='Bob', age=32, work='Developer'}
//    Persion{name='Jorge', age=30, work='Developer'}

  }


  /**
   * 读取集合
   * @param env
   */
  def fromCollection(env: ExecutionEnvironment): Unit = {
    val data = 1 to 10
//    传入一个集合(collection)
    env.fromCollection(data).print()
  }

  /**
   * 读取文件
   * @param env
   */
  def textFile(env: ExecutionEnvironment): Unit ={
//    指定文件
    val filePath = "D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\hello.txt"
//    指定文件夹
    val dirPath = "D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\inputs"
//    env.readTextFile(filePath).print()
    env.readTextFile(dirPath).print()
  }

}
