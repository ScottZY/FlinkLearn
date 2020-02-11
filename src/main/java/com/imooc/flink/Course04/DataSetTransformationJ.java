package com.imooc.flink.Course04;

import com.imooc.flink.scala.course04.DBUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

/**
 * dataSet Transformation
 */
public class DataSetTransformationJ {
    public static void main(String[] args) throws Exception {
//        构建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        filterFunction(env);
//        mapPartitionFunction(env);
        firstFunction(env);
    }


    /**
     * flatMap函数
     * @param env
     */
    public static void flatMMapFunction(ExecutionEnvironment env){

    }



    /**
     * first函数
     * @param env
     * @throws Exception
     */
    public static void firstFunction(ExecutionEnvironment env) throws Exception{
        List<Tuple2<Integer, String>> info = new ArrayList<Tuple2<Integer, String>>();
        info.add(new Tuple2<Integer, String>(1, "hadoop"));
        info.add(new Tuple2<Integer, String>(1, "Spark"));
        info.add(new Tuple2<Integer, String>(1, "Flink"));
        info.add(new Tuple2<Integer, String>(2, "Java"));
        info.add(new Tuple2<Integer, String>(2, "Ruby"));
        info.add(new Tuple2<Integer, String>(3, "Linux"));
        info.add(new Tuple2<Integer, String>(3, "Unix"));
        final DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);
        System.out.println("~~~~~~~~~~~~我是分割线~~~~~~~~~~~~~");
        data.first(3).print();
        System.out.println("~~~~~~~~~~~~我是分割线~~~~~~~~~~~~~");
        data.groupBy(0).first(2).print();
        System.out.println("~~~~~~~~~~~~我是分割线~~~~~~~~~~~~~");
        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
    }


    /**
     * mapPartition function
     * @param env
     */
    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<String>();
        for (int i = 1; i <= 100; i++){
            list.add("Students: "+i);
        }
        DataSource<String> data = env.fromCollection(list).setParallelism(6);
//        使用 mapPartition 创建41次
        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                String conn = DBUtils.getConnection();
                System.out.println(conn + "...");
                DBUtils.returnFunction(conn);
            }
        }).print();
    }

    /**
     * filter function
     */
    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        //        构建数据集
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i<=10; i++){
            list.add(i);
        }
        DataSource<Integer> dataSource = env.fromCollection(list);
        dataSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input+1;
            }
        }).filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer input) throws Exception {
                return input > 3;
            }
        }).print();
    }


    /**
     * Map Function 对数据集中的每一个元素做操作
     * @param env
     */
    public static void mapFunction(ExecutionEnvironment env) throws Exception {
//        构建数据集
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i<=10; i++){
            list.add(i);
        }
        DataSource<Integer> dataSource = env.fromCollection(list);
        dataSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer input) throws Exception {
                return input+1;
            }
        }).print();
    }
}
