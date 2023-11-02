package com.atguigu.window.fun.full;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @Author 城北徐公
 * @Date 2023/10/17-10:44
 * 求窗口内vc之和，并且将窗口的开始和结束时间拼接到id中
 * 窗口函数：
 * 1.功能强大，可以拿到窗口信息（例如：窗口的开始和结束时间）
 * 调用规则
 * stream
 * .keyBy(<key selector>)
 * .window(<window assigner>)
 * .apply(new MyWindowFunction());
 */
public class FullWinFun {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分组开窗聚合
        SingleOutputStreamOperator<WaterSensor> fullWinFunDS = waterSensorDS
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                //todo 不需要状态编程，apply 就够了
                .apply(new WindowFunction<WaterSensor, WaterSensor, String, TimeWindow>() {
                    //                      key              窗口对象                      输入                            输出
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensor> out) throws Exception {
                        Double sumVc = 0.0D;
                        for (WaterSensor waterSensor : input) {
                            sumVc += waterSensor.getVc();
                        }

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        out.collect(new WaterSensor(
                                key + "_    " + sdf.format(window.getStart()) + "   " + sdf.format(window.getEnd()),
                                window.getEnd(),
                                sumVc
                        ));
                    }
                });

        //5.打印
        fullWinFunDS.print();

        //6.执行
        env.execute();
    }
}
