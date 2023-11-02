package com.atguigu.exer.clas;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/17-10:57
 * 求出vc之和，并且给id字段打上窗口开始时间的标记
 * 聚合步骤：
 * 调用reduce方法
 * 参数：
 * 1.使用reduce求和
 * 2.使用WindowFunction来打上标记
 */
public class ReduceAndFull {
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //5.使用Reduce实现增量聚合(里面既可以传入一个reduce的增量聚合方法，也可以传入一个window function的全量聚合)
        SingleOutputStreamOperator<WaterSensor> reduceAndFullDS = windowDS
                .reduce(new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                return new WaterSensor(value2.getId(),
                                        Math.max(value1.getTs(), value2.getTs()),
                                        Math.min(value2.getVc(), value1.getVc())
                                );
                            }
                        }, new WindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensor> out) throws Exception {
                                WaterSensor waterSensor = input.iterator().next();
                                waterSensor.setId(waterSensor.getId() + "_" + window.getStart());
                                out.collect(waterSensor);
                            }
                        }
                );

        //6.打印
        reduceAndFullDS.print();

        //7.执行
        env.execute();


    }
}
