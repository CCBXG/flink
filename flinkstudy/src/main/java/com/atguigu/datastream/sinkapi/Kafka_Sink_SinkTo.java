package com.atguigu.datastream.sinkapi;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import com.atguigu.common.FlinkConstant;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @Author 城北徐公
 * @Date 2023/10/13-18:03
 */
public class Kafka_Sink_SinkTo {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.创建kafkaSink
        KafkaSink<WaterSensor> kafkaSink = KafkaSink.<WaterSensor>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setRecordSerializer(new KafkaRecordSerializationSchema<WaterSensor>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(WaterSensor element, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        return new ProducerRecord<>("test", JSON.toJSONString(element).getBytes());
                    }
                })
                .build();

        //5.向kafka中写入数据
        waterSensorDS.sinkTo(kafkaSink);

        //6.启动
        env.execute();

    }
}
