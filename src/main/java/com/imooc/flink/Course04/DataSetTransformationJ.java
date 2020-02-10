package com.imooc.flink.Course04;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

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
        filterFunction(env);
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
