package com.atguigu.state.keyed.list;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author 城北徐公
 * @Date 2023/10/18-16:51
 * 取每个传感器vc的Top3
 */
public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.分组
        KeyedStream<WaterSensor, String> keyByDS = waterSensorDS.keyBy(WaterSensor::getId);

        //5.针对每种传感器输出最高的3个水位值
        SingleOutputStreamOperator<String> top3 = keyByDS.map(new RichMapFunction<WaterSensor, String>() {
            private ListState<Double> listState;
            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Double>(
                        "Top3", Double.class));
            }
            /* 整体思路：
             * 只要有数据，就放入listState
             * 将其转储到arrayList中
             * 对arrayList数据排序
             * 取前min(3,arrayList.size()),输出Top3
             * 多余的元素进行删除
             * */
            @Override
            public String map(WaterSensor value) throws Exception {
                //将数据添加进listState
                Double thisVc = value.getVc();
                listState.add(thisVc);
                //将其转储到arrayList中
                ArrayList<Double> list = new ArrayList<>();
                for (Double vc : listState.get()) {
                    list.add(vc);
                }
                //排序
                list.sort(new Comparator<Double>() {
                    @Override
                    public int compare(Double o1, Double o2) {
                        return o2.compareTo(o1);
                    }
                });
                //取数据并拼接  清空listState多于3个的数据
                listState.clear();
                StringBuilder builder = new StringBuilder(value.getId()).append("的Top3:");
                for (int i = 0; i < Math.min(3, list.size()); i++) {
                    builder.append(list.get(i));
                    listState.add(list.get(i));
                    //为每个数据之间加一个",",最后一个数据后不加
                    if (i < Math.min(3, list.size()) - 1) {
                        builder.append(",");
                    }
                }

                return builder.toString();
            }
        });

        //6.打印
        top3.print();

        //7.启动
        env.execute();


    }
}
