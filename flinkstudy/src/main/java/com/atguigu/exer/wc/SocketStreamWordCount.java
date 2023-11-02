package com.atguigu.exer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/9-20:13
 * 无界流处理
 */
public class SocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        //todo 1.创建执行环境
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);

        //todo 2.获取端口连接
        DataStreamSource<String> socketTextStream = evn.socketTextStream("hadoop102", 9999);

        //todo 3.扁平化，分组，求和
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split(" ");
                        for (String s : split) out.collect(Tuple2.of(s, 1));
                    }
                })
                .keyBy(data -> data.f0)
                .sum(1)
                .print();

        //todo 4.启动
        evn.execute();
    }
}
