package com.atguigu.datastream.sinkapi;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author 城北徐公
 * @Date 2023/10/13-20:10
 */
public class JDBC_Sink {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.使用JDBC连接MySQL数据库
        waterSensorDS.keyBy(WaterSensor::getId)
                .max("vc")
                .addSink(JdbcSink.sink(" INSERT INTO ws2\n" +
                                "VALUES\n" +
                                "\t( ?,?,? ) ON DUPLICATE KEY UPDATE `vc` = ?",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setDouble(3, waterSensor.getVc());
                        preparedStatement.setDouble(4, waterSensor.getVc());
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)  //5条数据为1批次写入
                        .withBatchIntervalMs(10000)  //或者10秒写入
                        .withMaxRetries(2)  //错误最大重试次数
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("000000")
                        .build()
                ));

        //5.启动任务
        env.execute();
    }
}
