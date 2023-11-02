package com.atguigu.state.keyed.value;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author 城北徐公
 * @Date 2023/10/18-16:51
 * 针对同一传感器,如果连续的两条水位线数据值都超过10,则输出报警信息侧输出流
 */
public class ValueStateDemo2 {
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

        //5.针对同一传感器,如果连续的两条水位线数据差值超过10,则输出报警信息侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("warn"){};
        SingleOutputStreamOperator<WaterSensor> processDS = keyByDS.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            //声明状态,用于存储VC数据
            private ValueState<Double> valueState;
            //调用open方法初始化state
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>(
                        "vc-state", Double.class));
            }
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                //获取中间状态的值
                Double lastVC = valueState.value();
                Double thisVC = value.getVc();
/*                //如果状态为空，一定不输出警戒信息
                if (lastVC == null){
                    //如果本次大于10,则更新中间状态
                    if (thisVC>=10){
                        valueState.update(thisVC);
                    }
                }else {
                    if (thisVC>=10){
                        valueState.update(thisVC);
                        ctx.output(outputTag,"连续两次水位线超过了警戒值,vc1:"+lastVC+",vc2:"+thisVC);
                    }else {
                        valueState.clear();
                    }
                }*/


                if (thisVC >= 10){
                    if (lastVC != null)
                        ctx.output(outputTag,"连续两次水位线超过了警戒值,vc1:"+lastVC+",vc2:"+thisVC);
                    valueState.update(thisVC);
                }else {
                    valueState.clear();
                }


                out.collect(value);

            }
        });

        //6.打印
        processDS.print("主流数据-->");
        processDS.getSideOutput(outputTag).print("测流数据输出-->");

        //7.启动
        env.execute();


    }
}
