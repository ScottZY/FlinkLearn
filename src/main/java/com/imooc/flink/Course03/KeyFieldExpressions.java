package com.imooc.flink.Course03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 使用java API开发 Flink 的实时处理应用程序
 * 指定key为定义的类中的属性
 * wordcount统计的数据源于socket
 */
public class KeyFieldExpressions {
    public static void main(String[] args) throws Exception {
//        step1:创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        step2：读取数据
        DataStreamSource<String> text = env.socketTextStream("192.168.50.205", 9999);
//        step3：transform
        text.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> out) throws Exception {
                String[] tokens = value.split(",");
                for (String token : tokens) {
                    if (token.length() > 0){
                        out.collect(new WC(token, 1));
                    }
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(5)).sum("count").print().setParallelism(1);
//        WC{word='A', count=5}
//        WC{word='B', count=6}
//        WC{word='C', count=7}
//        WC{word='D', count=6}
//        timeWindow：设定多长时间执行一次
//        执行
        env.execute("StreamingWCJavaApp");
    }

    public static class WC{
        private String word;
        private int count;

//      定义有参构造和无参构造
        public WC(){}
        public WC(String word, int count){
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }


    }
}
