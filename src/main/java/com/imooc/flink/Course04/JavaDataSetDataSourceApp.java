package com.imooc.flink.Course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import java.util.ArrayList;
import java.util.List;

/**
 * Java DataSet API编程
 */
public class JavaDataSetDataSourceApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollection(env); //读取集合
        textFile(env);  // 读取文件或者文件夹
    }


    /**
     * 读取文件或文件夹
     * @param env
     */
    public static void textFile(ExecutionEnvironment env) throws Exception {
//        文件路径
        String filepath = "D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\hello.txt";
//        文件夹路径
        String dirPath = "D:\\idea_workSpace\\Flink_Learn\\flink-train-JandS\\src\\main\\resources\\inputs";
        env.readTextFile(dirPath).print();
    }

    /**
     * 读取集合
     * @param env
     */
    public static void fromCollection(ExecutionEnvironment env){
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i<10; i++){
            list.add(i);
        }
        try {
            env.fromCollection(list).print();
        } catch (Exception e) {
            System.out.println("代码错误");
            e.printStackTrace();
        }
    }
}
