package com.atguigu.flinkdoris;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/31-11:19
 * 使用DataStream读取数据
 */
public class StreamWritString {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L); //必须打开checkpoint

        //2.获取端口连接         1    1   zhangSan  1
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.连接Doris
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("hadoop102:7030") //地址
                .setTableIdentifier("test_db.table1") //库名表名
                .setUsername("root") //用户
                .setPassword("000000") //密码
                .build();
        DorisSink<String> dorisSink = DorisSink.<String>builder()
                .setDorisOptions(dorisOptions) //连接参数
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder()
                        .setDeletable(false) //是否删除表
                        .disable2PC() //开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setLabelPrefix("doris-") //前缀
                        .setBufferCount(3) // 批次条数: 默认 3条
                        .setBufferSize(8*1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔 : 默认 3秒      三个对批次的限制是或的关系
                        .setMaxRetries(3) //如果失败最多重试三次
                        .build()) //执行参数
                .setSerializer(new SimpleStringSerializer()) //序列化参数
                .build();

        //4.将数据写出
        socketTextStream.sinkTo(dorisSink);

        //5.启动任务
        env.execute();

    }
}
