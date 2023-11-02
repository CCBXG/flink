package com.atguigu.datastream.sourceapi;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author 城北徐公
 * @Date 2023/10/10-20:13
 */
public class SourceKafkaAdd {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        evn.setParallelism(1);

        //2.用addSource连接kafka数据源
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");//地址
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group1");//消费者组
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);//消费主题，序列化，配置信息
        DataStreamSource<String> kafkaDS = evn.addSource(kafkaConsumer);

        //3.输出数据
        kafkaDS.print();

        //4.启动
        evn.execute();

    }
}
