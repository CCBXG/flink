package com.atguigu.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author 城北徐公
 * @Date 2023/10/16-19:04
 * 自定义触发器
 */
public class MyTrigger extends Trigger<Object, TimeWindow> {
    //调用时机：当有新的元素到达窗口时，这个方法被调用。
    //触发条件：如果窗口的最大事件时间戳早于或等于当前水印时间（ctx.getCurrentWatermark()），则立即触发窗口计算，返回 TriggerResult.FIRE。
    //否则，注册事件时间计时器以在窗口的最大事件时间戳时触发，同时返回 TriggerResult.CONTINUE，表示继续等待。
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        //          9.999                    当前水位线
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            ctx.registerProcessingTimeTimer(window.maxTimestamp() + 2000L + 30000L);//自己设置的处理时间触发器要大于12，哪怕一毫秒
            return TriggerResult.CONTINUE;
        }
    }
    //调用时机：当事件时间计时器触发时，这个方法被调用。
    //触发条件：如果事件时间等于窗口的最大事件时间戳，则返回 TriggerResult.FIRE 表示触发窗口计算，否则返回 TriggerResult.CONTINUE 表示继续等待。
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());//如果触发了处理时间触发器，就关掉事件时间触发器
        return TriggerResult.FIRE;
    }
    //调用时机：当处理时间计时器触发时，这个方法被调用。
    //触发条件：在这个特定的触发器中，处理时间计时器触发时不执行任何操作，只返回 TriggerResult.CONTINUE。
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time == window.maxTimestamp()){
            ctx.deleteProcessingTimeTimer(window.maxTimestamp() + 2000L + 30000L);//如果触发了事件时间触发器，就关掉处理时间触发器
            return TriggerResult.FIRE;
        }else {
            return TriggerResult.CONTINUE;
        }
    }
    //调用时机：在窗口合并时，这个方法被调用。
    //触发条件：只有在水印尚未超过合并窗口的结束时间时才注册计时器。这与 onElement 方法中的逻辑一致。如果水印已经超过窗口的结束时间，onElement 方法将触发，而在这里设置计时器将导致窗口触发两次。
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp() + 2000L + 30000L);
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
