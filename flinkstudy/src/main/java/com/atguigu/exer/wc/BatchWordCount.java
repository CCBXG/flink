package com.atguigu.exer.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionEnvironmentFactory;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/9-16:43
 * 批处理
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //todo 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //todo 2.从文件读取数据
        DataSource<String> lineDS = env.readTextFile("input/word.txt");
        //lineDS.print();

        //todo 3.进行切分
        FlatMapOperator<String, String> wordDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });
        //wordDS.print();

        //todo 4.每个单词都用tuple2封装
        MapOperator<String, Tuple2<String, Integer>> wordOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });
        //wordOneDS.print();

        //todo 5.统计个数
        wordOneDS.groupBy(0).sum(1).print();


    }
}
