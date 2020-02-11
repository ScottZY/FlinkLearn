package com.imooc.flink.Course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * 使用java API开发Flink的批处理程序
 */
public class BatchWCJavaApp {
    public static void main(String[] args){
//        step1：获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String input = "/home/yongzhang/IdeaProjects/MeLearnFlink/FlinkLearn/src/main/resources/hello.txt";

//        step2：read data
        DataSource<String> text = env.readTextFile(input);

//        step3: transform
        try {
//            输出读取到的内容
            text.print();

//            下面进行词频统计：
//            java中的flatMap中传入的参数需要是一个类(函数)
//            参数含义：   进入类型：String      转换为某种类型：Tuple2<String, Integer>
            text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    String[] tokens = value.toLowerCase().split("\t");
                    for (String token : tokens) {
                        if (token.length() > 0){
//                            通过Collector(收集器)收集为 Tuple2<String, Integer>类型 收集每个单词的值为 -> token, 2
                            collector.collect(new Tuple2<String, Integer>(token, 1));
                        }else {
                            System.out.println("读取数据文件为空");
                        }
                    }
                }
            }).groupBy(0).sum(1).print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
