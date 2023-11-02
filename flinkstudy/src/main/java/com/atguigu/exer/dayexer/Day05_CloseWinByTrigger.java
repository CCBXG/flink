package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensorWM;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author 城北徐公
 * @Date 2023/10/18-8:45
 * 自定义触发器解决凌晨没有数据导致最后一个窗口无法关闭问题
 */
public class Day05_CloseWinByTrigger {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.提取时间戳生成水位线
        SingleOutputStreamOperator<WaterSensor> wmDS = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)) //乱序容忍程度
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        //5.分组开窗聚合
        SingleOutputStreamOperator<WaterSensorWM> triggerDS = wmDS
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                return new WaterSensor(value1.getId(), 0L, Math.max(value1.getVc(), value2.getVc()));
                            }
                        }, new WindowFunction<WaterSensor, WaterSensorWM, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensorWM> out) throws Exception {
                                int i = 0;
                                WaterSensor waterSensor = input.iterator().next();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                out.collect(new WaterSensorWM(
                                        String.valueOf(i++),
                                        waterSensor.getVc(),
                                        sdf.format(window.getStart()),
                                        sdf.format(window.getEnd())
                                ));
                            }
                        }
                );

        //6.打印
        triggerDS.print();

        //7.启动
        env.execute();

    }
}
