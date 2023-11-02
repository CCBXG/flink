package com.atguigu.datastream.sourceapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/10-21:19
 */
public class SourceKafkaFrom {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //2.连接kafka(KafkaSource.<String>builder()这个反省为什么要加上？)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setValueOnlyDeserializer(new SimpleStringSchema()) //序列化
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")//集群地址
                .setGroupId("group1")//消费之组
                .setTopics("test")//消费主题
                .setStartingOffsets(OffsetsInitializer.earliest())//消费策略
                .build();
        DataStreamSource<String> kafkaDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.输出数据
        kafkaDS.print();

        //4.启动
        env.execute();

    }
}
