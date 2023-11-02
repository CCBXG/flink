package com.atguigu.datastream.transforapi;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/12-20:53
 */
public class TransRepartition {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        
        //3.使用map使其并行化
        SingleOutputStreamOperator<String> mapDS = socketTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });
        mapDS.print("mapDS>>>>>");

        //4.重分区并设置并行度
        //随机分区（shuffle）
        //mapDS.shuffle().print("shuffle>>>>>").setParallelism(4);
        //轮询分区（rebalance）
        mapDS.rebalance().print("rebalance>>>>>").setParallelism(4);

        //5.启动任务
        env.execute();



    }
}
