package com.atguigu.datastream.transforapi;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author 城北徐公
 * @Date 2023/10/12-21:19
 *
 */
public class TransSideOutPut {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转为WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分流
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("output"){};//outputTag为分流标记
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getVc() <= 30.0D) {
                    //支流输出
                    ctx.output(outputTag, value);
                } else {
                    //主流输出
                    out.collect(value);
                }
            }
        });

        //5.打印流
        //打印支流
        processDS.getSideOutput(outputTag).print("sendDS>>>>>>");
        //打印主流
        processDS.print("processDS>>>>>>");

        //6.启动任务
        env.execute();


    }
}
