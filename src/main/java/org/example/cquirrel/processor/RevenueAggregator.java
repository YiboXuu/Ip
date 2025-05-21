package org.example.cquirrel.processor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.cquirrel.model.DataModels.QueryResult;
import org.example.cquirrel.model.RevenueUpdate;

/**
 * 收入聚合器 - 按 orderKey 聚合，只在最终状态输出一次结果
 * 解决：同一 orderKey 重复出现导致的多条记录问题
 */
public class RevenueAggregator
        extends KeyedProcessFunction<Integer, RevenueUpdate, QueryResult> {

    // 聚合期限，60秒后触发输出
    private static final long AGGREGATION_TIMEOUT_MS = 60_000L;

    // 每个订单的累计收入
    private transient ValueState<Double> revenueState;
    // 对应的订单日期
    private transient ValueState<java.util.Date> orderDateState;
    // 对应的发运优先级
    private transient ValueState<Integer> shipPriorityState;
    // 是否已注册定时器标志
    private transient ValueState<Boolean> timerRegistered;

    @Override
    public void open(Configuration parameters) {
        revenueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("revenueState", Double.class, 0.0)
        );
        orderDateState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("orderDateState", java.util.Date.class)
        );
        shipPriorityState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("shipPriorityState", Integer.class)
        );
        timerRegistered = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerRegistered", Boolean.class, false)
        );
    }

    @Override
    public void processElement(
            RevenueUpdate update,
            Context ctx,
            Collector<QueryResult> out) throws Exception {

        // 累加收入
        double current = revenueState.value();
        double total = current + update.getDeltaRevenue();

        if (Math.abs(total) < 1e-6) {
            // 收入归零时，清理所有状态，不再输出该订单
            revenueState.clear();
            orderDateState.clear();
            shipPriorityState.clear();
            timerRegistered.clear();
            System.out.println(
                    "Cleared orderKey=" + update.getOrderKey() + " (revenue ~ 0)");
        } else {
            // 更新状态
            revenueState.update(total);
            orderDateState.update(update.getOrderDate());
            shipPriorityState.update(update.getShipPriority());

            // 记录处理情况，但不立即输出
            System.out.println(String.format(
                    "Processing: orderKey=%d, deltaRevenue=%.2f, totalRevenue=%.2f",
                    update.getOrderKey(), update.getDeltaRevenue(), total));

            // 第一次收到该 key 的事件时，注册一个延迟输出的定时器
            if (!timerRegistered.value()) {
                long timer = ctx.timerService().currentProcessingTime() + AGGREGATION_TIMEOUT_MS;
                ctx.timerService().registerProcessingTimeTimer(timer);
                timerRegistered.update(true);
                System.out.println("Registered timer for orderKey=" + update.getOrderKey());
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<QueryResult> out) throws Exception {
        // 输出当前累计收入（最终状态）
        Double revenue = revenueState.value();
        if (revenue != null && revenue > 0) {
            QueryResult qr = new QueryResult(
                    ctx.getCurrentKey(),  // orderKey
                    revenue,
                    orderDateState.value(),
                    shipPriorityState.value()
            );
            out.collect(qr);

            System.out.println(String.format(
                    "Final output: orderKey=%d, revenue=%.2f",
                    ctx.getCurrentKey(), revenue));
        }
    }
}