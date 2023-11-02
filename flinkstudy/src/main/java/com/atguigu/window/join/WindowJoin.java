package com.atguigu.window.join;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author 城北徐公
 * @Date 2023/10/17-8:38
 * 对于Window Join
 *  1.join 操作必须开窗才能使用
 *  2.可以使用三种时间窗口
 *  3.可以使用两种时间语义
 */
public class WindowJoin {
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
                                return Long.parseLong(element.split(",")[1])*1000L;
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
                                return element.getTs()*1000L;
                            }
                        })
                );

        //4.使用滚动窗口对两条流进行join
        //join使用前提：（1）开窗，（2）只能使用三种时间窗口
        // 1. 落在同一个时间窗口范围内才能匹配
        // 2. 根据keyby的key，来进行匹配关联
        DataStream<Tuple3<String, String, Double>> resultDS = waterSensorDS
                .join(strDS) //waterSensorDS join strDS
                .where(WaterSensor::getId) //waterSensorDS 的 key
                .equalTo(value -> value.split(",")[0]) //strDS 的 key
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) //使用的开窗方式
                .apply(new JoinFunction<WaterSensor, String, Tuple3<String, String, Double>>() {
                    @Override
                    public Tuple3<String, String, Double> join(WaterSensor first, String second) throws Exception {
                        return new Tuple3<>(first.getId(), second.split(",")[2], first.getVc());
                    }
                });

        //5.输出
        resultDS.print();

        //6.启动
        env.execute();


    }
}
