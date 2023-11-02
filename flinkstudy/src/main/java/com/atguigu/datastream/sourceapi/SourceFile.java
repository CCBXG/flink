package com.atguigu.datastream.sourceapi;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/10-19:35
 */
public class SourceFile {
    public static void main(String[] args) throws Exception {
        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.使用fromSource读取数据（只要是用了Stream就要.build()）
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),    //读取方式：按行
                new Path("hdfs://hadoop102:8020/text/word.txt")    //参数可以是目录，也可以是文件；还可以从HDFS目录下读取
        ).build();
        DataStreamSource<String> fileDS = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file-source");

        //3.输出
        fileDS.print();

        //4.启动
        env.execute();


    }
}
