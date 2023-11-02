package com.atguigu.window.assigners;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @Author 城北徐公
 * @Date 2023/10/13-13:56
 */
public class CountSlidingWindow {
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

        //5.计数滑动窗口，窗口大小6，滑动步长3
        WindowedStream<WaterSensor, String, GlobalWindow> windowDS = keyByDS.countWindow(6, 3);

        //6.求水位线最大值
        SingleOutputStreamOperator<WaterSensor> sumVc = windowDS.max("vc");

        //7.打印
        sumVc.print();

        //8.启动
        env.execute();



    }
}
