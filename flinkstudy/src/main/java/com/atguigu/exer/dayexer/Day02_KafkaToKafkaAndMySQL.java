package com.atguigu.exer.dayexer;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import com.atguigu.common.FlinkConstant;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author 城北徐公
 * @Date 2023/10/13-9:23
 */
public class Day02_KafkaToKafkaAndMySQL {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.连接kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("group1")
                .setTopics("input")
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        DataStreamSource<String> socketTextStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.将数据转为WaterSensor对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分流操作
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("side"){};
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() { //processDS总流
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if (value.getVc() < 30.0D) {
                    out.collect(value);
                } else {
                    ctx.output(outputTag, value);
                }
            }
        });

        //5.主流数据输出到kafka(带序列化器的)
        KafkaSink<WaterSensor> kafkaSink = KafkaSink.<WaterSensor>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setRecordSerializer(new KafkaRecordSerializationSchema<WaterSensor>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor waterSensor, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        return new ProducerRecord<>("test", JSON.toJSONBytes(waterSensor));
                    }
                }).build();
        processDS.sinkTo(kafkaSink); //processDS是主流

        //6.测流数据输出到MySQL
        SideOutputDataStream<WaterSensor> sideDS = processDS.getSideOutput(outputTag); //获取支流
        SinkFunction<WaterSensor> jdbcSink = JdbcSink.sink("INSERT INTO ws2 VALUES ( ?,?,?) ON DUPLICATE KEY UPDATE `vc` = ? ",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setDouble(3, waterSensor.getVc());
                        preparedStatement.setDouble(4, waterSensor.getVc());
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(2)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(2)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
        );
        sideDS.keyBy(WaterSensor::getId)
                .max("vc")
                .addSink(jdbcSink);//sideDS是测流

        //7.启动
        env.execute();


    }
}
