package com.atguigu.state.checkpoint;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/20-19:38
 */
public class CheckPointDemo {
    public static void main(String[] args) {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        //1.1 开启CheckPoint
        env.enableCheckpointing(5000L); //生产环境设置为分钟 10Min

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //1.2存储目录
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/flink/ck-0524");
        //1.3级别(精准一次，至少一次)
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

    }
}
