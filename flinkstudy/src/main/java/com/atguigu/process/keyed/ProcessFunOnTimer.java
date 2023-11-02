package com.atguigu.process.keyed;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author 城北徐公
 * @Date 2023/10/18-14:05
 * 只有“按键分区流”KeyedStream才支持设置定时器的操作
 */
public class ProcessFunOnTimer {
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

        //5.在接收到数据5秒以后输出一条数据,且将该数据输出到侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("output"){};
        SingleOutputStreamOperator<WaterSensor> processDS = keyByDS.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            //每个元素都会调用一次
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                //获取定时器服务对象(通过上下文对象获得)
                TimerService timerService = ctx.timerService();

                //获取当前处理时间
                long time = timerService.currentProcessingTime();

                //注册5秒之后的定时器  处理时间大于等于定时器时间就可以触发
                long timerTs = time + 5000L;
                System.out.println("ts:" + timerTs);
                timerService.registerProcessingTimeTimer(timerTs);
                out.collect(value);

            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, WaterSensor>.OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                System.out.println("onTimer:" + timestamp);
                ctx.output(outputTag, "5秒到了");

            }
        });

        //打印
        processDS.print("主流-->");
        processDS.getSideOutput(outputTag).print("测流-->");

        //启动任务
        env.execute();

    }
}
