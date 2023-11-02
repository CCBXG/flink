package com.atguigu.process.win;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @Author 城北徐公
 * @Date 2023/10/18-14:37
 * 计算并输出窗口内部水位线次数前两名
 */
public class ProcessAllWindowFun {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.直接开窗
        AllWindowedStream<WaterSensor, TimeWindow> winDS = waterSensorDS.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        //5.计算并输出窗口内部水位线次数前两名
        SingleOutputStreamOperator<String> processDS = winDS.process(new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                //使用hashmap来放置vc出现的次数
                HashMap<Double, Integer> vcCount = new HashMap<>();
                //遍历elements
                Iterator<WaterSensor> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    Double thisVc = iterator.next().getVc();
                    if (!vcCount.containsKey(thisVc)) {
                        vcCount.put(thisVc, 1);
                    } else {
                        vcCount.put(thisVc, vcCount.get(thisVc) + 1);
                    }
                }
                //转为list<tuple2>排序
                ArrayList<Tuple2<Double, Integer>> arrayList = new ArrayList<>();
                Set<Map.Entry<Double, Integer>> entries = vcCount.entrySet();
                for (Map.Entry<Double, Integer> entry : entries) {
                    arrayList.add(new Tuple2<>(entry.getKey(), entry.getValue()));
                }
                arrayList.sort(new Comparator<Tuple2<Double, Integer>>() {
                    @Override
                    public int compare(Tuple2<Double, Integer> o1, Tuple2<Double, Integer> o2) {
                        return o2.f1- o1.f1;
                    }
                });

                //输出
                StringBuilder stringBuilder = new StringBuilder("水位线Top2为：");
                for (int i = 0; i < Math.min(2,arrayList.size()); i++) {
                    Tuple2<Double, Integer> tuple2 = arrayList.get(i);
                    stringBuilder.append("\n" + tuple2.f0 + "出现了:" + tuple2.f1 + "次");
                }

                out.collect(stringBuilder.toString());
            }
        });

        //6.打印
        processDS.print();

        //7.执行
        env.execute();

    }
}
