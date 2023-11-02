package com.atguigu.function;

import com.atguigu.bean.VcAcc;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author 城北徐公
 * @Date 2023/10/24-21:02
 */
public class MyUDAF extends AggregateFunction<Double, VcAcc> {

    //初始化一个要返回的实体类
    @Override
    public VcAcc createAccumulator() {
        return new VcAcc("",0.0D,0);
    }

    //获取最后的结果
    @Override
    public Double getValue(VcAcc vcAcc) {
        return vcAcc.getSumVc()/vcAcc.getCountVc();
    }

    //累加器
    public void accumulate(VcAcc acc, Double value, Integer count) {
        acc.setSumVc(acc.getSumVc()+value);
        acc.setCountVc(acc.getCountVc()+1);
    }

    //撤回器
    public void retract(VcAcc acc, Double value, Integer count) {
        acc.setSumVc(acc.getSumVc()-value);
        acc.setCountVc(acc.getCountVc()-1);
    }

    //合流
    public void merge(VcAcc acc, Iterable<VcAcc> it) {
        for (VcAcc vcAcc : it) {
            acc.setSumVc(acc.getSumVc()+ vcAcc.getSumVc());
            acc.setCountVc(acc.getCountVc()+vcAcc.getCountVc());
        }
    }

    public void resetAccumulator(VcAcc acc) {
        acc.setSumVc(0.0D);
        acc.setCountVc(0);
        acc.setId("");
    }
}
