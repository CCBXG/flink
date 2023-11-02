package com.atguigu.exer.dayexer;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import com.atguigu.common.FlinkConstant;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @Author 城北徐公
 * @Date 2023/10/19-21:21
 * 	从Kafka topic1读取WaterSensor数据,从8888端口读取水位阈值数据(每个传感器可以单独设置阈值)做成广播流;
 * 	将WaterSensor中VC超过阈值的数据输出到侧输出流并在控制台打印;
 * 	将WaterSensor中VC不超过阈值的数据输出到主流并传输到Kafka topic2.
 */
public class Day07_BroadState {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("group1")
                .setTopics("topic1")
                .build();
        DataStreamSource<String> socketTextStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.从端口读取每个传感器阈值数据  1001,20   1002,50
        DataStreamSource<String> broadDS = env
                .socketTextStream("hadoop102", 8888);

        //5.将阈值数据做成广播状态
        MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<String, Double>(
                "map-state",String.class, Double.class);//key是要指定阈值的id，value是要指定的阈值
        BroadcastStream<String> broadcast = broadDS.broadcast(mapStateDescriptor);

        //6.将数据流与广播流进行连接
        BroadcastConnectedStream<WaterSensor, String> connectDS = waterSensorDS.connect(broadcast);

        //7.对连接流进行处理  分流
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("output") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = connectDS.process(new BroadcastProcessFunction<WaterSensor, String, WaterSensor>() {
            //主流的处理逻辑
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, WaterSensor>.ReadOnlyContext ctx, Collector<WaterSensor> out) throws Exception {
                ReadOnlyBroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                Double vc = broadcastState.get(value.getId());
                if (vc==null){
                    vc=30.0D;
                }

                if (value.getVc()>=vc){
                    ctx.output(outputTag,value);
                }else {
                    out.collect(value);
                }
            }

            //广播流的处理逻辑
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                //将阈值数据写入广播状态
                //先获取广播流状态
                BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //写入
                String[] split = value.split(",");
                broadcastState.put(split[0],Double.parseDouble(split[1]));
            }
        });


        //8.获取测流并打印
        KafkaSink<WaterSensor> kafkaSink = KafkaSink.<WaterSensor>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setRecordSerializer(new KafkaRecordSerializationSchema<WaterSensor>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor element, KafkaSinkContext context, Long timestamp) {
                        return new ProducerRecord<>("test", JSON.toJSONString(element).getBytes());
                    }
                })
                .build();

        processDS.getSideOutput(outputTag).print("warn-->");
        //processDS.print();
        processDS.sinkTo(kafkaSink);

        //9.启动任务
        env.execute();

    }
}
