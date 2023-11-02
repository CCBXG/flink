package com.atguigu.state.keyed.aggregating;

import com.atguigu.bean.VcAcc;
import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/19-20:17
 * 计算每种传感器的平均水位
 */
public class AggregatingStateDemo {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分组
        KeyedStream<WaterSensor, String> keyByDS = waterSensorDS.keyBy(WaterSensor::getId);

        //5.求平均值
        SingleOutputStreamOperator<WaterSensor> map = keyByDS.map(new RichMapFunction<WaterSensor, WaterSensor>() {
            private AggregatingState<Double, Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggregatingState = getRuntimeContext().getAggregatingState(new
                        AggregatingStateDescriptor<Double, VcAcc, Double>(
                        "agg-state",
                        new AggregateFunction<Double, VcAcc, Double>() {
                            @Override
                            public VcAcc createAccumulator() {
                                return new VcAcc("", 0.0D, 0);
                            }

                            @Override
                            public VcAcc add(Double value, VcAcc accumulator) {
                                accumulator.setSumVc(accumulator.getSumVc() + value);
                                accumulator.setCountVc(accumulator.getCountVc() + 1);
                                return accumulator;
                            }

                            @Override
                            public Double getResult(VcAcc accumulator) {
                                return accumulator.getSumVc() / accumulator.getCountVc();
                            }

                            @Override
                            public VcAcc merge(VcAcc a, VcAcc b) {
                                return null;
                            }
                        },
                        VcAcc.class
                ));

            }

            @Override
            public WaterSensor map(WaterSensor value) throws Exception {
                aggregatingState.add(value.getVc());
                value.setVc(aggregatingState.get());
                return value;
            }
        });

        //6.打印
        map.print();

        //7.启动
        env.execute();


    }
}
