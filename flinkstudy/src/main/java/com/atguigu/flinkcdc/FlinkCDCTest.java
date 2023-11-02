package com.atguigu.flinkcdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/25-11:41
 */
public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {
        //1.获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall")
                .tableList("gmall.base_trademark")
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema())  //序列化
                .startupOptions(StartupOptions.initial()) //监控blog的偏移量
                .build();
        DataStreamSource<String> cdcDS = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "flink-cdc");

        //
        cdcDS.print();

        //
        env.execute();

        
    }
}
