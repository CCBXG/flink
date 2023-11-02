package com.atguigu.exer.dayexer;

import com.atguigu.bean.WaterSensor;
import com.atguigu.function.StringToWaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author 城北徐公
 * @Date 2023/10/19-8:43
 * 统计最近10秒钟内出现次数最多的两个水位,并且每5秒钟更新一次
 */
public class Day06_WaterSensorTop2 {
    public static void main(String[] args) throws Exception {
        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取连接
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(new StringToWaterSensor());

        //4.开窗
        SingleOutputStreamOperator<Tuple3<Double, Integer, Long>> reduceDS = waterSensorDS
                //使用map给每个水位线封装成(vc，1)
                .map(new MapFunction<WaterSensor, Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> map(WaterSensor value) throws Exception {
                        return new Tuple2<>(value.getVc(), 1);
                    }
                })
                //按照vc进行分组
                .keyBy(value -> value.f0)
                //开窗
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .reduce(
                        //用ReduceFunction将不同vc出现的次数分别累加
                        new ReduceFunction<Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> reduce(Tuple2<Double, Integer> value1, Tuple2<Double, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                },
                        //使用ProcessWindowFunction为元素打上窗口结束时间戳标记
                        new ProcessWindowFunction<Tuple2<Double, Integer>, Tuple3<Double, Integer, Long>, Double, TimeWindow>() {
                    @Override
                    public void process(Double aDouble, ProcessWindowFunction<Tuple2<Double, Integer>, Tuple3<Double, Integer, Long>, Double, TimeWindow>.Context context, Iterable<Tuple2<Double, Integer>> elements, Collector<Tuple3<Double, Integer, Long>> out) throws Exception {
                        Tuple2<Double, Integer> next = elements.iterator().next();
                        out.collect(new Tuple3<>(next.f0, next.f1, context.window().getEnd()));
                    }
                });

        //5.按照窗口结束时间分组,将同一个窗口所有数据收集齐,排序取前两名输出
        SingleOutputStreamOperator<String> processDS = reduceDS
                .keyBy(value -> value.f2)
                //使用KeyedProcessFunction进行排序(这里面进入的数据就是同一个key,（同一个时间戳）的数据)
                .process(new KeyedProcessFunction<Long, Tuple3<Double, Integer, Long>, String>() {
                    private ArrayList<Tuple2<Double, Integer>> list = new ArrayList<>();

                    //将vc相同的数据放入同一个list，并且根据窗口的结束时间调用定时器
                    @Override
                    public void processElement(Tuple3<Double, Integer, Long> value, KeyedProcessFunction<Long, Tuple3<Double, Integer, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                        list.add(new Tuple2<>(value.f0, value.f1));
                        ctx.timerService().registerProcessingTimeTimer(value.f2 + 1L);
                    }

                    //调用定时器，定时器所做的任务
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Double, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        //1.排序
                        list.sort(new Comparator<Tuple2<Double, Integer>>() {
                            @Override
                            public int compare(Tuple2<Double, Integer> o1, Tuple2<Double, Integer> o2) {
                                //降序,后 - 前
                                return o2.f1 - o1.f1;
                            }
                        });
                        //2.输出前两名
                        StringBuilder builder = new StringBuilder().append("水位线Top2为：");
                        for (int i = 0; i < Math.min(2, list.size()); i++) {
                            Tuple2<Double, Integer> vcAndCount = list.get(i);
                            builder
                                    .append("\n")
                                    .append(vcAndCount.f0)
                                    .append("出现了")
                                    .append(vcAndCount.f1)
                                    .append("次");
                        }
                        //清空集合
                        list.clear();
                        out.collect(builder.toString());
                    }
                });

        //6.打印
        processDS.print();

        //7.输出
        env.execute();

    }
}
