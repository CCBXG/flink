package com.atguigu.state.op.broad;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author 城北徐公
 * @Date 2023/10/19-21:21
 * 水位超过指定的阈值发送告警，阈值可以动态修改。
 */
public class BroadStateDemo {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.创建广播流
        SingleOutputStreamOperator<Double> broadDS = env
                .socketTextStream("hadoop102", 8888)
                .map(Double::parseDouble);//将读入的string转为double

        //5.将阈值数据做成广播状态
        MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<String, Double>(
                "map-state",String.class, Double.class);//通过key读取value
        BroadcastStream<Double> broadcast = broadDS.broadcast(mapStateDescriptor);

        //6.结合数据流与阈值流(两条不同来源，不同状态的流放在一起，类似于一国两制)
        BroadcastConnectedStream<WaterSensor, Double> connectDS = waterSensorDS.connect(broadcast);

        //7.根据阈值对数据流进行分流处理（由于分流要用到测流，所以调process）
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("outPut"){};//泛型控制测流输出类型
        SingleOutputStreamOperator<WaterSensor> processDS = connectDS.process(new BroadcastProcessFunction<WaterSensor, Double, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, Double, WaterSensor>.ReadOnlyContext ctx, Collector<WaterSensor> out) throws Exception {
                //判断阈值是否存在，不存在则赋初始值
                ReadOnlyBroadcastState<String, Double> broadState = ctx.getBroadcastState(mapStateDescriptor);
                Double vcCritical = broadState.get("vcCritical");
                if (vcCritical == null) {
                    vcCritical = 30.0D;
                }

                //根据阈值进行分流
                if (value.getVc() >= vcCritical) {
                    ctx.output(outputTag, value);
                } else {
                    out.collect(value);
                }
            }

            @Override
            public void processBroadcastElement(Double value, BroadcastProcessFunction<WaterSensor, Double, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                //将阈值数据写入广播状态
                //先获取广播流状态
                BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //写入
                broadcastState.put("vcCritical", value);
            }
        });

        //8.获取测流并打印
        processDS.getSideOutput(outputTag).print("warn-->");
        processDS.print("result-->");

        //9.启动任务
        env.execute();

    }
}
