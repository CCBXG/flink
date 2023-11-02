package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @Author 城北徐公
 * @Date 2023/10/23-20:34
 * DataStream实现计算每分钟出现传感器的个数(即按照ID去重之后有几种不同的传感器)
 */
public class Day09_WaterSensorId_Count_Window02 {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream
                .map(new StringToWaterSensor());

        //4.分组开窗去重
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> reduceDS = waterSensorDS
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return value1;
                    }
                }, new WindowFunction<WaterSensor, Tuple3<String, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        //获取窗口信息
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                        out.collect(new Tuple3<>(
                                sdf.format(window.getStart()),
                                sdf.format(window.getEnd()),
                                1
                        ));
                    }
                });


        //5.按照窗口信息进行分组求和
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> processDS = reduceDS
                .keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
                    private ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Integer> value, KeyedProcessFunction<String, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>.Context ctx, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        TimerService timerService = ctx.timerService();
                        Integer count = valueState.value();
                        //如果某一个窗口的数据第一次来，创建定时器
                        if (count == null) {
                            timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 10);
                            valueState.update(1);
                        } else {
                            //否则，计数器加一
                            count++;
                            valueState.update(count);
                        }
                    }
                    //定时器
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>.OnTimerContext ctx, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        out.collect(new Tuple3<>(ctx.getCurrentKey(), null, valueState.value()));
                        valueState.clear();
                    }
                });

        //6.打印
        processDS.print();

        //7.启动任务
        env.execute();

    }
}
