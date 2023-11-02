package com.atguigu.flinksql;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author 城北徐公
 * @Date 2023/10/24-14:35
 */
public class TableToStream {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEvn = StreamTableEnvironment.create(env);

        //2.建表
        tableEvn.executeSql("CREATE TABLE KafkaTable (\n" +
                "`id` STRING,\n" +
                "`ts` BIGINT,\n" +
                "`vc` DOUBLE\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'test',\n" +
                "'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "'properties.group.id' = 'group1',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");

        //3.查询
        Table table = tableEvn.sqlQuery("select * from KafkaTable");

        //4.转为stream流
        //涉及到追加流使用toDataStream
        DataStream<WaterSensor> dataStream = tableEvn.toDataStream(table, WaterSensor.class);
        dataStream.print();
        //涉及到撤回流使用toChangelogStream
        DataStream<Row> changelogStream = tableEvn.toChangelogStream(table);
        changelogStream.print();

        //5.执行
        env.execute();

    }
}
