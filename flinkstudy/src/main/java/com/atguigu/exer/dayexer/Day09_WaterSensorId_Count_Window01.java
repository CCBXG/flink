package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.HashSet;

/**
 * @Author 城北徐公
 * @Date 2023/10/23-20:34
 * DataStream实现计算每分钟出现传感器的个数(即按照ID去重之后有几种不同的传感器)
 */
public class Day09_WaterSensorId_Count_Window01 {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.开窗
        AllWindowedStream<WaterSensor, TimeWindow> windowAll = waterSensorDS.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //5.统计按照id去重后有几种不同的传感器
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> applyDS = windowAll.apply(new AllWindowFunction<WaterSensor, Tuple3<String, String, Integer>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<WaterSensor> values, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                //对value进行遍历去重
                HashSet<String> ids = new HashSet<>();
                for (WaterSensor value : values) {
                    ids.add(value.getId());
                }

                //获取窗口信息
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                out.collect(new Tuple3<>(
                        sdf.format(window.getStart()),
                        sdf.format(window.getEnd()),
                        ids.size()
                ));
            }
        });

        //6.打印
        applyDS.print();

        //7.启动任务
        env.execute();


    }
}
