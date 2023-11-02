package com.atguigu.flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/24-14:10
 */
public class FlinkSQLTest {
    public static void main(String[] args) {
        //1.创建环境变量
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

        //4.执行
        table.execute().print();

    }
}
