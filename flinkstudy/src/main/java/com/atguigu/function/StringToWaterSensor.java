package com.atguigu.function;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author 城北徐公
 * @Date 2023/10/12-21:14
 * map方法，将字符串转为WaterSensor对象
 */
public class StringToWaterSensor implements MapFunction<String, WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0],Long.parseLong(split[1]),Double.parseDouble(split[2]));
    }
}
