package com.atguigu.state.keyed.map;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @Author 城北徐公
 * @Date 2023/10/19-14:44
 * 统计每种传感器每种水位值出现的次数
 */
public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.按照id分组
        KeyedStream<WaterSensor, String> keyByDS = waterSensorDS.keyBy(WaterSensor::getId);

        //5.根据id分组后，求每个vc出现的次数
        SingleOutputStreamOperator<String> map = keyByDS.map(new RichMapFunction<WaterSensor, String>() {
            private MapState<Double, Integer> mapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Double, Integer>(
                        "map-state", Double.class, Integer.class));
            }
            @Override
            public String map(WaterSensor value) throws Exception {
                Double thisVC = value.getVc();
                if (mapState.contains(thisVC)) {
                    mapState.put(thisVC, mapState.get(thisVC) + 1);
                } else {
                    mapState.put(thisVC, 1);
                }
                //取出mapState里面的所有值，遍历输出
                Iterable<Map.Entry<Double, Integer>> entries = mapState.entries();
                StringBuilder builder = new StringBuilder().append("传感器id号:" + value.getId() + "\n");
                for (Map.Entry<Double, Integer> entry : entries) {
                    builder.append(entry.getKey()).append("水位出现了").append(entry.getValue()).append("次\n");
                }
                return builder.toString();
            }
        });

        //输出
        map.print();

        //执行任务
        env.execute();

    }
}
