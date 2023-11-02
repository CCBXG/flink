package com.atguigu.exer.dayexer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import com.atguigu.common.FlinkConstant;
import com.atguigu.function.StringToWaterSensor;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
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
 * @Date 2023/10/27-9:16
 * 	从Kafka topic1读取WaterSensor数据,使用FlinkCDC监控MySQL以获取水位阈值数据(每个传感器可以单独设置阈值)做成广播流;
 * 	将WaterSensor中VC超过阈值的数据输出到侧输出流并在控制台打印;
 * 	将WaterSensor中VC不超过阈值的数据输出到主流并传输到Kafka topic2.
 */
public class Day12_VcMonitor {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.kafkaSource读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("group1")
                .setTopics("topic1")
                .build();
        SingleOutputStreamOperator<WaterSensor> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source").map(new StringToWaterSensor());

        //3.使用FlinkCDC监控MySQL
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .databaseList("test")
                .tableList("test.vc_monitor")
                .username("root")
                .password("000000")
                .build();
        DataStreamSource<String> mySqlStream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");

        mySqlStream.print();

        //4.将mysql中的变化的阈值作为广播流
        MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, Double.class);
        BroadcastStream<String> broadcast = mySqlStream.broadcast(mapStateDescriptor);

        //5.数据流与广播流进行连接
        BroadcastConnectedStream<WaterSensor, String> connect = kafkaStream.connect(broadcast);

        //6.对连接流处理，由于要分流要用到测流，因此需要调用带有状态的函数
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("output") {
        };
        SingleOutputStreamOperator<WaterSensor> processDS = connect.process(new BroadcastProcessFunction<WaterSensor, String, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, WaterSensor>.ReadOnlyContext ctx, Collector<WaterSensor> out) throws Exception {
                ReadOnlyBroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                //通过broadcastState获取要处理id的vc阈值，并附上默认值
                Double vcMonitor = broadcastState.get(value.getId());
                if (vcMonitor == null) {
                    vcMonitor = 30.0D;
                }
                //id根据vc阈值对其进行分流
                if (value.getVc() >= vcMonitor) {
                    ctx.output(outputTag, value);
                } else {
                    out.collect(value);
                }
            }

            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {

                //将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);

                //获取操作类型
                String op = jsonObject.getString("op");

                //获取广播状态数据
                BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                //如果为删除操作,则删除状态中的对应数据,否则插入
                if ("d".equals(op)) {
                    //{"id":"1001","vc":30.0}
                    JSONObject before = JSON.parseObject(jsonObject.getString("before"));
                    broadcastState.remove(before.getString("id"));
                } else {
                    JSONObject after = JSON.parseObject(jsonObject.getString("after"));
                    broadcastState.put(after.getString("id"), after.getDouble("vc"));
                }

            }
        });

        //7.输出
        KafkaSink<WaterSensor> kafkaSink = KafkaSink.<WaterSensor>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(new KafkaRecordSerializationSchema<WaterSensor>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor element, KafkaSinkContext context, Long timestamp) {
                        return new ProducerRecord<>("topic2", JSON.toJSONString(element).getBytes());
                    }
                })
                .build();
        processDS.getSideOutput(outputTag).print("warn");
        processDS.sinkTo(kafkaSink);

        //8.启动任务
        env.execute();


    }
}
