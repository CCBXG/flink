package com.atguigu.exer.dayexer;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/25-9:32
 * IDEA中代码实现,从主题1和主题2分别读取数据创建表,从MySQL中加载表,将1 2进行JOIN并与MySQL表进行LookUpJoin
 */
public class Day11_LookUpJoin {
    public static void main(String[] args) {
        //1.创建环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEvn = StreamTableEnvironment.create(env);

        //建表
        //创建KafkaTable1表
        tableEvn.executeSql("" +
                "CREATE TABLE KafkaTable1 (\n" +
                "`id` STRING,\n" +
                "`name` STRING,\n" +
                "`age` BIGINT,\n" +
                "`pt` AS PROCTIME() \n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'topic1',\n" +
                "'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "'properties.group.id' = 'group1',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");
        //创建KafkaTable2表
        tableEvn.executeSql("" +
                "CREATE TABLE KafkaTable2 (\n" +
                "`id` STRING,\n" +
                "`cout` STRING,\n" +
                "`score` BIGINT\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "'topic' = 'topic2',\n" +
                "'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "'properties.group.id' = 'group1',\n" +
                "'scan.startup.mode' = 'latest-offset',\n" +
                "'format' = 'csv'\n" +
                ")");
        //创建维度表ws
        tableEvn.executeSql("" +
                "CREATE TABLE ws (\n" +
                "`id` STRING,\n" +
                "`ts` BIGINT,\n" +
                "`vc` DOUBLE\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://hadoop102:3306/test',\n" +
                "  'table-name' = 'ws1',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '000000',\n" +
                "  'lookup.cache' = 'PARTIAL',\n" +
                "  'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                "  'lookup.partial-cache.expire-after-access' = '1 hour',\n" +
                "  'lookup.partial-cache.max-rows' = '10'\n" +
                "  )");

        //sql语句
        Table table = tableEvn.sqlQuery("" +
                "select\n" +
                "t1.id,\n" +
                "name,\n" +
                "age,\n" +
                "cout,\n" +
                "score,\n" +
                "ws.vc,\n" +
                "t1.pt\n" +
                "from(\n" +
                "select \n" +
                "k1.id id, \n" +
                "k1.name name,\n" +
                "k1.age age,\n" +
                "k2.cout cout,\n" +
                "k2.score score,\n" +
                "k1.pt pt\n" +
                "from KafkaTable1 k1\n" +
                "join KafkaTable2 k2\n" +
                "on k1.id=k2.id\n" +
                ") t1\n" +
                "join ws\n" +
                "FOR SYSTEM_TIME AS OF t1.pt AS ws\n" +
                "on t1.id = ws.id;");

        //4.执行
        table.execute().print();




    }
}
