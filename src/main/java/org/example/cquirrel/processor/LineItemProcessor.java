package org.example.cquirrel.processor;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.cquirrel.model.DataModels.LineItem;
import org.example.cquirrel.model.DataModels.UpdateEvent;
import org.example.cquirrel.model.DataModels.UpdateType;
import org.example.cquirrel.model.OrderInfo;
import org.example.cquirrel.model.RevenueUpdate;
import org.example.cquirrel.util.DateUtil;

public class LineItemProcessor
        extends KeyedBroadcastProcessFunction<
        Integer,                   // key 类型：按 l_orderkey 分区
        UpdateEvent<LineItem>,     // 主输入：LineItem 的 INSERT/DELETE
        OrderInfo,                 // 广播输入：OrderInfo
        RevenueUpdate              // 输出：RevenueUpdate
        > {

    // 用来存 OrderInfo 的广播状态 descriptor
    private final MapStateDescriptor<Integer, OrderInfo> orderStateDesc =
            new MapStateDescriptor<>(
                    "OrderBroadcastState",
                    Integer.class,
                    OrderInfo.class
            );

    // cutoff date 参数，比如 "1995-03-15"
    private final String cutoffDate;

    public LineItemProcessor(String cutoffDate) {
        this.cutoffDate = cutoffDate;
    }

    @Override
    public void open(Configuration parameters) {
        // 本例不需要额外的键控状态
    }

    @Override
    public void processBroadcastElement(
            OrderInfo orderInfo,
            Context ctx,
            Collector<RevenueUpdate> out) throws Exception {
        // 更新广播状态：按 orderKey 存储最新的 OrderInfo（含 orderDate、shipPriority、isValid）
        ctx.getBroadcastState(orderStateDesc)
                .put(orderInfo.getOrderKey(), orderInfo);

        System.out.println(String.format(
                "Order broadcast received: key=%d, date=%s, priority=%d, isValid=%b",
                orderInfo.getOrderKey(),
                orderInfo.getOrderDate(),
                orderInfo.getShipPriority(),
                orderInfo.isValid()
        ));
    }

    @Override
    public void processElement(
            UpdateEvent<LineItem> event,
            ReadOnlyContext ctx,
            Collector<RevenueUpdate> out) throws Exception {

        LineItem li = event.getData();
        int orderKey = li.getL_orderkey();

        // 检查 shipdate 是否晚于 cutoffDate
        boolean lineValid = DateUtil.isAfter(li.getL_shipdate(), cutoffDate);

        if (event.getType() == UpdateType.INSERT) {
            if (!lineValid) {
                // 船期不符，直接跳过
                System.out.println("LineItem skipped (shipdate≤cutoff): " + orderKey + "_" + li.getL_linenumber());
                return;
            }

            // 从广播状态里拿 OrderInfo
            OrderInfo oi = ctx.getBroadcastState(orderStateDesc).get(orderKey);
            if (oi == null || !oi.isValid()) {
                // 订单无效或还未到达，跳过
                System.out.println("LineItem skipped (no valid OrderInfo): " + orderKey);
                return;
            }

            // 计算 revenue
            double rev = li.getL_extendedprice() * (1 - li.getL_discount());
            RevenueUpdate upd = new RevenueUpdate(
                    orderKey,
                    rev,
                    oi.getOrderDate(),
                    oi.getShipPriority()
            );
            out.collect(upd);

            System.out.println(String.format(
                    "LineItem processed: %d_%d → +%.2f",
                    orderKey, li.getL_linenumber(), rev
            ));
        }
        else if (event.getType() == UpdateType.DELETE) {
            // 删除时，如果之前有效则发负值
            if (lineValid) {
                OrderInfo oi = ctx.getBroadcastState(orderStateDesc).get(orderKey);
                if (oi != null && oi.isValid()) {
                    double rev = - li.getL_extendedprice() * (1 - li.getL_discount());
                    RevenueUpdate upd = new RevenueUpdate(
                            orderKey,
                            rev,
                            oi.getOrderDate(),
                            oi.getShipPriority()
                    );
                    out.collect(upd);

                    System.out.println(String.format(
                            "LineItem deleted: %d_%d → %.2f",
                            orderKey, li.getL_linenumber(), rev
                    ));
                }
            }
        }
    }
}