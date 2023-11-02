package com.atguigu.window.fun.increment;

import com.atguigu.bean.VcAcc;
import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author 城北徐公
 * @Date 2023/10/17-10:26
 * 求平均水位线
 * aggregate需要实现四个方法
 * createAccumulator()：对中间状态的初始化
 * add()：对中间状态进行聚合操作
 * getResult()：对最终结果进行操作输出
 */
public class AggregateFun {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分组开窗聚合
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = waterSensorDS
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //5.聚合操作
        SingleOutputStreamOperator<WaterSensor> avgVC = windowDS.aggregate(new AggregateFunction<WaterSensor, VcAcc, WaterSensor>() {
            //中间状态的初始化
            @Override
            public VcAcc createAccumulator() {
                System.out.println("我是对中间状态初始化的函数-->createAccumulator()");
                return new VcAcc("", 0D, 0);
            }

            //进行累加
            @Override
            public VcAcc add(WaterSensor value, VcAcc accumulator) {
                System.out.println("我是对中间状态进行处理的函数-->add()");
                return new VcAcc(
                        value.getId(),
                        accumulator.getSumVc() + value.getVc(),
                        accumulator.getCountVc() + 1);
            }

            //获取最终结果的方法
            @Override
            public WaterSensor getResult(VcAcc accumulator) {
                System.out.println("我是计算结果的函数-->getResult()");
                return new WaterSensor(
                        "avgVC",
                        System.currentTimeMillis(),
                        accumulator.getSumVc() / accumulator.getCountVc()
                );
            }

            @Override
            public VcAcc merge(VcAcc a, VcAcc b) {
                System.out.println("merge>>>>");
                return new VcAcc(a.getId(),
                        a.getSumVc() + b.getSumVc(),
                        a.getCountVc() + b.getCountVc()
                );
            }
        });

        //6.打印结果
        avgVC.print();

        //7.启动
        env.execute();

    }
}
