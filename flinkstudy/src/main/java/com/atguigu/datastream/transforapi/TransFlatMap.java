package com.atguigu.datastream.transforapi;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/12-19:06
 * 过滤掉不是json格式的数据
 */
public class TransFlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        
        //3.flatmap也可以实现过滤功能
        //过滤掉不是json格式的数据
        SingleOutputStreamOperator<JSONObject> filterDS = socketTextStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    //如果不是json字符串则会抛异常
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("脏数据:" + value);
                }
            }
        });

        //4.打印
        filterDS.print();

        //5.执行
        env.execute();

    }
}
