package com.atguigu.exer.clas;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensorWM;
import com.atguigu.common.FlinkConstant;
import com.atguigu.function.StringToWaterSensor;
import com.atguigu.trigger.MyTrigger;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author 城北徐公
 * @Date 2023/10/16-14:18
 * 自定义触发器
 */
public class KafkaToKafka {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从kafka读取数据并且转为JavaBean对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setGroupId("timer")
                .setTopics("test")
                .build();
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kf-sou")
                .map(new StringToWaterSensor());

        //3.提取时间戳生成waterMake
        SingleOutputStreamOperator<WaterSensor> waterSensorWMDS = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))  //设置乱序时间
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() { //提取时间戳
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() ;
                    }
                }));

        //4.分组开窗聚合
        SingleOutputStreamOperator<WaterSensorWM> resultDS = waterSensorWMDS
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new MyTrigger())
                .reduce(new ReduceFunction<WaterSensor>() {
                            @Override
                            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                                return new WaterSensor(value1.getId(), 0L, value1.getVc() + value2.getVc());
                            }
                        }, new WindowFunction<WaterSensor, WaterSensorWM, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensorWM> out) throws Exception {
                                //前面使用了增量聚合，只可能产生一条数据
                                WaterSensor waterSensor = input.iterator().next();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                out.collect(new WaterSensorWM(waterSensor.getId(),
                                        waterSensor.getVc(),
                                        sdf.format(window.getStart()),
                                        sdf.format(window.getEnd()))
                                );
                            }
                        }
                );

        //5.打印
        resultDS.print();

        //6.启动
        env.execute();

    }
}
