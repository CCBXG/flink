package com.atguigu.exer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/9-19:52
 * 有界流处理
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //todo 1.创建环境
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1); //设置并行度为1

        //todo 2.读取文件
        DataStreamSource<String> textFile = evn.readTextFile("input/word.txt");

        //todo 3.转换、分组、求和，得到统计结果
        textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split(" ");
                        for (String s : split) {
                            out.collect(Tuple2.of(s, 1));
                        }
                    }
                })
                .keyBy(data -> data.f0)
                .sum(1)      //二元组的位置
                .print();

        //todo 4.流式处理需要启动任务
        evn.execute();
    }
}
