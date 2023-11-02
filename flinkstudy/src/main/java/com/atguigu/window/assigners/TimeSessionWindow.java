package com.atguigu.window.assigners;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author 城北徐公
 * @Date 2023/10/13-13:56
 */
public class TimeSessionWindow {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.按照主键分组
        KeyedStream<WaterSensor, String> keyByDS = waterSensorDS.keyBy(WaterSensor::getId);

        //5.时间会话窗口，会话时长为3秒
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = keyByDS
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)));

        //6.求水位线最大值
        SingleOutputStreamOperator<WaterSensor> sumVc = windowDS.max("vc");

        //7.打印
        sumVc.print();

        //8.启动
        env.execute();



    }
}
