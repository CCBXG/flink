package com.atguigu.state.keyed.reducing;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/19-18:50
 * 计算每种传感器的水位和
 */
public class ReduceStateDemo {
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

        //5.聚合
        SingleOutputStreamOperator<WaterSensor> mapDS = keyByDS.map(new RichMapFunction<WaterSensor, WaterSensor>() {
            private ReducingState<Double> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>(
                        "reducing-state",
                        new ReduceFunction<Double>() {
                            @Override
                            public Double reduce(Double value1, Double value2) throws Exception {
                                return value1 + value2;
                            }
                        },
                        Double.class
                ));
            }

            @Override
            public WaterSensor map(WaterSensor value) throws Exception {
                reducingState.add(value.getVc());
                //更新最后输出的一个bean里面的vc值
                value.setVc(reducingState.get());
                return value;
            }
        });

        //6.输出
        mapDS.print();

        //7.启动
        env.execute();
    }
}
