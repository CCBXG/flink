package com.atguigu.process.fun;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/17-16:10
 */
public class WordCount {
    public static void main(String[] args) throws Exception {

        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平
        SingleOutputStreamOperator<String> word = socketTextStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String word : split) {
                    out.collect(word);
                }
            }
        });

        //4.转为二元组并且设置为1
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = word.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value, 1));
            }
        });

        //5.分组
        KeyedStream<Tuple2<String, Integer>, String> keyByWordOne = wordOne.keyBy(value -> value.f0);

        //6.聚合计算单词出现的次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCou = keyByWordOne
                .process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            //定义一个中间数据状态
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //每一个中间状态的参数
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer count = valueState.value();
                if (count == null) { //如果该单词首次出现，则count即valueState.value()的值为空
                    out.collect(value);//直接输出该二元组
                    valueState.update(1); //并且把中间计数状态的参数设置为一
                } else {
                    count++; //如果不是第一次出现，则把count的值加一
                    valueState.update(count); //更新count值
                    out.collect(new Tuple2<>(value.f0, count)); //输出一次该二元组

                }
            }
        });

        //7.打印
        wordCou.print();

        //8.执行
        env.execute();


    }
}
