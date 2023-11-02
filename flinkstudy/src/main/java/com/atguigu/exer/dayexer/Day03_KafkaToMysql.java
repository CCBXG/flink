package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensorWin;
import com.atguigu.common.FlinkConstant;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

/**
 * @Author 城北徐公
 * @Date 2023/10/14-8:39
 * 实操：从Kafka读取数据,按照传感器ID分组,开10秒滚动窗口,计算窗口内最高水位线,
 * 并将结果写出到MySQL(MySQL表字段为:id--传感器ID,stt--窗口开始时间,edt--窗口结束时间,vc--最高水位线)
 */
public class Day03_KafkaToMysql {
    public static void main(String[] args) throws Exception {
        //1.获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从kafka读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(FlinkConstant.BOOTSTRAP_SERVER)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setGroupId("group1")
                .setTopics("test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        DataStreamSource<String> socketTextStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        //3.分组开窗
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = socketTextStream.map(new StringToWaterSensor())
                .keyBy(WaterSensor::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //4.计算窗口内逻辑
        SingleOutputStreamOperator<WaterSensorWin> reduceDS = windowDS
                .reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(),
                        Math.max(value1.getTs(), value2.getTs()),
                        Math.max(value1.getVc(), value2.getVc())
                );
            }
        }, new WindowFunction<WaterSensor, WaterSensorWin, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<WaterSensorWin> out) throws Exception {
                WaterSensor waterSensor = input.iterator().next();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                WaterSensorWin waterSensorWin = new WaterSensorWin(waterSensor.getId(), waterSensor.getTs(), waterSensor.getVc(),
                        sdf.format(window.getStart()), sdf.format(window.getEnd()));
                out.collect(waterSensorWin);
            }
        });

        //5.写出到mysql
        SinkFunction<WaterSensorWin> mysqlSink = JdbcSink.sink("INSERT INTO ws3 VALUES ( ?,?,?,?,?) ",
                new JdbcStatementBuilder<WaterSensorWin>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensorWin waterSensorWin) throws SQLException {
                        preparedStatement.setString(1, waterSensorWin.getId());
                        preparedStatement.setLong(2, waterSensorWin.getTs());
                        preparedStatement.setDouble(3, waterSensorWin.getVc());
                        preparedStatement.setString(4, waterSensorWin.getStart());
                        preparedStatement.setString(5, waterSensorWin.getEnd());
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(2)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("000000")
                        .build());
        reduceDS.addSink(mysqlSink);

        //6.启动
        env.execute();

    }
}
