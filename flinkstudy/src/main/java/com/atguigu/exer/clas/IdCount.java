package com.atguigu.exer.clas;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author 城北徐公
 * @Date 2023/10/23-9:01
 * DataStream实现计算出现传感器的个数(即按照ID去重之后有几种不同的传感器)
 */
public class IdCount {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分组去重
        SingleOutputStreamOperator<Integer> processDS = waterSensorDS
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
                    private ValueState<String> valueState;
                    private Integer count = 0;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                                "value-state", String.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                        if (valueState.value() == null) {
                            valueState.update("1");
                            out.collect(++count);
                        }

                    }
                });

        //5.打印
        processDS.print("传感器种类:");

        //6.执行任务
        env.execute();

    }
}
