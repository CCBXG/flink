package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author 城北徐公
 * @Date 2023/10/21-9:09
 * 从端口读取WaterSensor数据,对数据按照id去重进行输出,
 * 即:如果1001已经输出过,再来1001数据则输出到侧输出流
 */
public class Day08_DeDup {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.去重
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("outPut") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    private MapState<String, WaterSensor> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                                "mapState", String.class, WaterSensor.class
                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = value.getId();
                        if (mapState.contains(id)) {
                            ctx.output(outputTag, value);
                        } else {
                            mapState.put(value.getId(), value);
                            out.collect(value);
                        }

                    }
                });

        //5.输出
        processDS.getSideOutput(outputTag).print("重复数据-->");
        processDS.print("主流-->");

        //6.执行任务
        env.execute();

    }
}
