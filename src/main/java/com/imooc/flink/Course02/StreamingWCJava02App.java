package com.imooc.flink.Course02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用java API开发 Flink 的实时处理应用程序
 *
 * wordcount统计的数据源于socket
 */
public class StreamingWCJava02App {

    public static void main(String[] args) throws Exception {
//        设置参数
        int port = 0;
        try {
            ParameterTool tools = ParameterTool.fromArgs(args);
            port = tools.getInt("port");
//            设置为：  --port 9998  详见 Run/Debug Configurations中 Program arguments中的设置
            System.out.println("使用设置的端口为："+port);
        }catch (Exception e){
            System.err.println("端口未设置，使用默认参数9999");
            port=9999;
        }

//        step1:创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("localhost", port);
//        step3：transform
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.split(",");
                for (String token : tokens) {
                    if (token.length() > 0){
                        out.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1);
//        timeWindow：设定多长时间执行一次
//        执行
        env.execute("StreamingWCJava02App");
    }
}
