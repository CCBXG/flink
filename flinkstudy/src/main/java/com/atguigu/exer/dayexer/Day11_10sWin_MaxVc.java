package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author 城北徐公
 * @Date 2023/10/25-9:43
 * IDEA中代码实现,从端口加载数据(1001,1,12),使用FlinkSQL实现10秒滚动窗口最大VC需求
 */
public class Day11_10sWin_MaxVc {
    public static void main(String[] args) {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.流转表(使用视图进行流转表，同时该表命名为ws)
        Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("ts", DataTypes.BIGINT())
                .column("vc", DataTypes.DOUBLE())
                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)") //东八区
                .build();
        tableEnv.createTemporaryView("ws",waterSensorDS);

        //5.



    }
}
