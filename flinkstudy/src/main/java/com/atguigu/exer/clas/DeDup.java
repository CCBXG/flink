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
 * DataStream实现计算每分钟出现传感器的去重
 */
public class DeDup {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分组去重
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                                "value-state", String.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        //每个key单独对应一个valueState
                        //如果状态没有值,说明该key第一次出现,收集到结果中,并且打上标记,再次遇见就不再收集
                        if (valueState.value() == null) {
                            out.collect(value);
                            valueState.update("1");
                        }
                    }
                });

        //5.打印
        processDS.print();

        //6.执行任务
        env.execute();

    }
}
