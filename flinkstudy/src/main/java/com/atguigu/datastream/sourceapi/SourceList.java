package com.atguigu.datastream.sourceapi;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * @Author 城北徐公
 * @Date 2023/10/10-19:18
 */
public class SourceList {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);

        //2.创建一个数据源
        List<String> list = Arrays.asList("aaa", "bbb", "ccc");

        //3.source使用fromCollection
        DataStreamSource<String> listDS = evn.fromCollection(list);

        //4.打印
        listDS.print();

        //5.启动
        evn.execute();

    }
}
