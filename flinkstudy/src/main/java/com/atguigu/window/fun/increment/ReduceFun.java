package com.atguigu.window.fun.increment;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author 城北徐公
 * @Date 2023/10/17-9:52
 * 对于一个窗口算子而言，窗口分配器和窗口函数是必不可少的
 *
 * 归约函数
 * 1.开窗流调用
 * 2.属于增量聚合
 */
public class ReduceFun {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分组开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = waterSensorDS
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))); //没有提取事件时间，所以使用TumblingProcessingTimeWindows

        //5.实现增量聚合
        SingleOutputStreamOperator<WaterSensor> reduceDS = windowDS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(
                        value2.getId(),
                        Math.max(value1.getTs(), value2.getTs()),
                        Math.min(value2.getVc(), value1.getVc())
                );
            }
        });

        //6.输出
        reduceDS.print();

        //7.启动
        env.execute();
    }
}
