package com.atguigu.udf;

import com.atguigu.function.MyUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/24-20:31
 */
public class TableFun {
    public static void main(String[] args) {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.建表
        tableEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "`id` STRING\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'test',\n" +
                "'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "'properties.group.id' = 'group1',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");

        //3.注册
        tableEnv.createTemporarySystemFunction("my_udtf", MyUDTF.class);

        //4.查询
        tableEnv.sqlQuery("select id,word1,word2 from KafkaTable, LATERAL TABLE(my_udtf(id))")
                .execute()
                .print();
    }
}
