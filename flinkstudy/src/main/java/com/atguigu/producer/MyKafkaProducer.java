package com.atguigu.producer;

import com.atguigu.common.FlinkConstant;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Author 城北徐公
 * @Date 2023/10/16-14:19
 */
public class MyKafkaProducer {
    public static void main(String[] args) throws InterruptedException {

        //创建一个kafka生产者
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, FlinkConstant.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        int i=0;
        while (true){

            kafkaProducer.send(new ProducerRecord("test","1001,"+System.currentTimeMillis()+",1.0"));
            System.out.println(++i);
            Thread.sleep(1000L); //线程睡眠一秒

        }

    }
}
