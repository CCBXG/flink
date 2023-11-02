package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensorWM;
import com.atguigu.common.FlinkConstant;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @Author 城北徐公
 * @Date 2023/10/16-9:11
 * 从Kafka读取数据,按照事件时间开窗(10秒滚动窗口),计算每个传感器的水位线之和,
 * 乱序程度2秒,允许迟到2秒,再迟到数据进入侧输出流。将结果写出到MySQL(侧输出流数据也需要手动更新到MySQL)。
 * MySQL字段为：
 * stt:窗口开始时间
 * edt:窗口结束时间
 * id:传感器ID
 * vc:水位线之和
 */
public class Day04_KafkaToMysql {
    public static void main(String[] args) throws Exception {
        //1.获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.连接kafka
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("group1")
                .setTopics("test")
                .build();
        DataStreamSource<String> socketTextStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.转为javabean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.提取时间戳生成waterMake
        SingleOutputStreamOperator<WaterSensor> wMDS = waterSensorDS
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)) //乱序时间两秒
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() { //拿到时间戳
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000L;
                                    }
                                }));

        //TODO 5.分组开窗聚合
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("late") {
        };
        SingleOutputStreamOperator<WaterSensorWM> resultDS = wMDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), 0L, value1.getVc() + value2.getVc());
                    }
                }, new WindowFunction<WaterSensor, WaterSensorWM, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensorWM> out) throws Exception {
                        WaterSensor waterSensor = input.iterator().next();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        out.collect(new WaterSensorWM(
                                waterSensor.getId(),
                                waterSensor.getVc(),
                                sdf.format(window.getStart()),
                                sdf.format(window.getEnd())
                        ));
                    }
                });

        //6.获得测流数据
        SideOutputDataStream<WaterSensor> outPutDS = resultDS.getSideOutput(outputTag);
        outPutDS.print();

        //7.将测输出流处理成与主流相同的数据流
        SingleOutputStreamOperator<WaterSensorWM> sideDS = outPutDS.map(new MapFunction<WaterSensor, WaterSensorWM>() {
            @Override
            public WaterSensorWM map(WaterSensor value) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                //TimeWindow.getWindowStartWithOffset(时间戳映射器)是通过找源码TumblingEventTimeWindows得到的
                long start = TimeWindow.getWindowStartWithOffset(value.getTs() * 1000L, 0L, 10 * 1000L);
                return new WaterSensorWM(
                        value.getId(),
                        value.getVc(),
                        sdf.format(start),
                        sdf.format(start + 10 * 1000L)
                );
            }
        });

        //8.将数据写出到MySQL
        //主流数据输出
        resultDS.addSink(JdbcSink.<WaterSensorWM>sink("INSERT INTO ws2 VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE `vc`=?",
                new JdbcStatementBuilder<WaterSensorWM>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensorWM waterSensorWM) throws SQLException {
                        preparedStatement.setString(1, waterSensorWM.getId());
                        preparedStatement.setDouble(2, waterSensorWM.getVc());
                        preparedStatement.setString(3, waterSensorWM.getStart());
                        preparedStatement.setString(4, waterSensorWM.getEnd());
                        preparedStatement.setDouble(5, waterSensorWM.getVc());
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withMaxRetries(2)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()));

        //侧流数据输出
        sideDS.addSink(JdbcSink.<WaterSensorWM>sink("INSERT INTO ws2 VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE `vc`=`vc`+?",
                new JdbcStatementBuilder<WaterSensorWM>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensorWM waterSensorWM) throws SQLException {
                        preparedStatement.setString(1, waterSensorWM.getId());
                        preparedStatement.setDouble(2, waterSensorWM.getVc());
                        preparedStatement.setString(3, waterSensorWM.getStart());
                        preparedStatement.setString(4, waterSensorWM.getEnd());
                        preparedStatement.setDouble(5, waterSensorWM.getVc());
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withMaxRetries(2)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()));

        //8.启动
        env.execute();

    }
}
