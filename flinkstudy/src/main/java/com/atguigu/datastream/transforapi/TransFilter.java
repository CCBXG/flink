package com.atguigu.datastream.transforapi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/12-14:05
 * 过滤出flink单词
 */
public class TransFilter {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.过滤器
/*        SingleOutputStreamOperator<String> filterDS = socketTextStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.equals("flink");
            }
        });*/

        //只要时单词时spark的
        SingleOutputStreamOperator<String> filterDS = socketTextStream.filter(value -> value.equals("flink"));

        //4.输出
        filterDS.print();

        //5.执行
        env.execute();
    }
}
