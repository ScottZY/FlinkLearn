package com.imooc.flink.Course04;

import com.imooc.flink.scala.course04.DBUtils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
//        firstFunction(env);
//        flatMMapFunction(env);
//        distinctFunction(env);
        joinFunction(env);
    }

    /**
     * join 函数
     * @param env
     */
    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2<Integer, String>(1, "Montreal"));
        info1.add(new Tuple2<Integer, String>(2, "Toronto"));
        info1.add(new Tuple2<Integer, String>(3, "Shanghai"));
        info1.add(new Tuple2<Integer, String>(4, "York"));
        info1.add(new Tuple2<Integer, String>(6, "Beijing"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<Integer, String>(1, "James"));
        info2.add(new Tuple2<Integer, String>(2, "Will"));
        info2.add(new Tuple2<Integer, String>(3, "Reymond"));
        info2.add(new Tuple2<Integer, String>(4, "Jack"));
        info2.add(new Tuple2<Integer, String>(5, "XiaoMing"));

        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(info2);
//        join操作
//        left data, right data, output data
        source1.join(source2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> left, Tuple2<Integer, String> right) throws Exception {
                return new Tuple3<>(left.f0, right.f1, left.f1);
            }
        }).print();

//        (3,Reymond,Shanghai)
//        (1,James,Montreal)
//        (2,Will,Toronto)
//        (4,Jack,York)
    }

    /**
     * distinct 函数
     * @param env
     * @throws Exception
     */
    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<String>();
        list.add("hadoop,flink");
        list.add("hadoop,Spark");
        list.add("Linux,Spark");
        DataSource<String> source = env.fromCollection(list);
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] strings = s.split(",");
                for (String string : strings) {
                    collector.collect(string);
                }
            }
        }).distinct().print();
    }

    /**
     * flatMap函数 词频统计
     * @param env
     */
    public static void flatMMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<String>();
        list.add("hadoop, flink");
        list.add("hadoop, Spark");
        list.add("Linux, Spark");

        DataSource<String> data = env.fromCollection(list);
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(", ");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).groupBy(0).sum(1).print();


//   另一种写法
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        List<String> list = new ArrayList<String>();
//        list.add("hadoop, flink");
//        list.add("hadoop, Spark");
//        list.add("Linux, Spark");
//        DataSource<String> source = env.fromCollection(list);
//        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] split = s.split(", ");
//                for (String s1 : split) {
//                    collector.collect(new Tuple2<>(s1, 1));
//                }
//            }
//        }).groupBy(0).sum(1).print();
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
