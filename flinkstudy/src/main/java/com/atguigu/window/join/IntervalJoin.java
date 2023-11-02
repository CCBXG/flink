package com.atguigu.window.join;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/16-20:52
 * 对于IntervalJoin
 * 1.必须开窗
 * 2.必须bukey
 * 3.时间语义只能是事件处理时间
 */
public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取两条需要join的流，提取时间戳
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 8888);
        //strDS字段  id,ts,tag  exp:1001,5,s_1
        SingleOutputStreamOperator<String> strDS = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forMonotonousTimestamps() //默认的乱序时间容忍度为0
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() { //序列化时间戳分配器
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Long.parseLong(element.split(",")[1]) * 1000L;
                            }
                        })
                );

        //3.两条流一个用String，一个用JavaBean
        //waterSensorDS字段  id,ts,vc  exp:1001,5,6
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream1
                .map(new StringToWaterSensor())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        })
                );

        //4.使用滚动窗口对两条流进行interval join
        //是基于KeyedStream的联结（join）操作,即必须先keyby操作
        SingleOutputStreamOperator<Tuple3<String, String, Double>> resultDS = waterSensorDS
                .keyBy(WaterSensor::getId) //先对waterSensorDS进行keyby
                .intervalJoin(strDS.keyBy(value -> value.split(",")[0])) //对strDS进行keyby
                .between(Time.seconds(-5), Time.seconds(5)) //设置连结间隔时间   [ts-5,ts+5]左闭右闭
                .process(new ProcessJoinFunction<WaterSensor, String, Tuple3<String, String, Double>>() {
                    @Override
                    public void processElement(WaterSensor left, String right, ProcessJoinFunction<WaterSensor, String, Tuple3<String, String, Double>>.Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                        out.collect(new Tuple3<>(left.getId(), right.split(",")[2], left.getVc()));
                    }
                }); //处理逻辑

        //5.输出
        resultDS.print();

        //6.启动
        env.execute();


    }
}
