package com.atguigu.flinkdoris;

import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @Author 城北徐公
 * @Date 2023/10/31-11:51
 */
public class StreamRead {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取连接
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("hadoop102:7030") //连接地址
                .setTableIdentifier("test_db.sales_records") //数据库及表
                .setUsername("root") //用户
                .setPassword("000000") //密码
                .build();
        DorisSource<List<?>> dorisSource = DorisSourceBuilder.<List<?>>builder()
                .setDorisOptions(dorisOptions) //doris连接描述器
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDeserializer(new SimpleListDeserializationSchema()) //序列化
                .build();
        DataStreamSource<List<?>> dorisStreamSource = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris-source");

        //打印到控制台
        dorisStreamSource.print();

        //启动任务
        env.execute();

    }
}
