package com.atguigu.datastream.transforapi;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/12-19:29
 * 获取水位线中的最大值
 */
public class TransMax {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.创建读取器
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将string转为bean对象
        SingleOutputStreamOperator<WaterSensor> mapDS = socketTextStream.map((MapFunction<String, WaterSensor>) value -> {
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        //4.对id进行分组,找到每个id中的最大水位
        KeyedStream<WaterSensor, String> keyByDS = mapDS.keyBy(WaterSensor::getId);  //键控流
        SingleOutputStreamOperator<WaterSensor> maxVc = keyByDS.max("vc");  //键控流调用聚合函数后变为普通数据流

        //5.打印
        maxVc.print();

        //6.执行
        env.execute();

    }
}
