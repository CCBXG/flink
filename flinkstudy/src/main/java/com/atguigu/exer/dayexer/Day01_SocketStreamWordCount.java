package com.atguigu.exer.dayexer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/10-9:05
 */
public class Day01_SocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取端口连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平，分组，求和，输出
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(value, 1)); //输入端到输出端处理
                    }
                })
                .keyBy(key -> key.f0)  //按照二元组的第一个元素进行分组
                .sum(1)  //二元组的第二个元素累加
                .print();

        //4.启动
        env.execute();
    }
}
