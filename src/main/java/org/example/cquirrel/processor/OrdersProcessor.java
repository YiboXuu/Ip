package org.example.cquirrel.processor;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.cquirrel.model.CustomerInfo;
import org.example.cquirrel.model.DataModels.UpdateEvent;
import org.example.cquirrel.model.DataModels.UpdateType;
import org.example.cquirrel.model.DataModels.Orders;
import org.example.cquirrel.model.OrderInfo;
import org.example.cquirrel.util.DateUtil;

public class OrdersProcessor
        extends KeyedBroadcastProcessFunction<
        Integer,                     // key 类型：按 custKey 分区
        UpdateEvent<Orders>,         // 主输入：Orders 的 INSERT/DELETE
        CustomerInfo,                // 广播输入：Customer 信息
        OrderInfo                    // 输出：带有效标志的 OrderInfo
        > {

    // 广播状态里存 customerInfo 的 descriptor
    private final MapStateDescriptor<Integer, CustomerInfo> customerStateDesc =
            new MapStateDescriptor<>(
                    "CustomerBroadcastState",
                    Integer.class,
                    CustomerInfo.class
            );

    // cutoff date 参数（形如 "1995-03-15"）
    private final String cutoffDate;

    public OrdersProcessor(String cutoffDate) {
        this.cutoffDate = cutoffDate;
    }

    @Override
    public void open(Configuration parameters) {
        // 本例不需要额外的 keyed state
    }

    @Override
    public void processElement(
            UpdateEvent<Orders> event,
            ReadOnlyContext ctx,
            Collector<OrderInfo> out) throws Exception {

        Orders order = event.getData();
        int orderKey    = order.getO_orderkey();
        int custKey     = order.getO_custkey();

        // 1) 先查 broadcastState 拿到 customer
        CustomerInfo custInfo = ctx
                .getBroadcastState(customerStateDesc)
                .get(custKey);

        // 2) 判断日期是否满足 cutoff
        boolean orderValid = DateUtil.isBefore(order.getO_orderdate(), cutoffDate);

        // 3) 对新增/删除分开处理
        if (event.getType() == UpdateType.INSERT) {
            boolean isValid = orderValid
                    && custInfo != null
                    && custInfo.isValid();

            // 一定要把 o_orderdate 和 o_shippriority 传下去
            out.collect(new OrderInfo(
                    orderKey,
                    custKey,
                    order.getO_orderdate(),
                    order.getO_shippriority(),
                    isValid
            ));

            System.out.println(String.format(
                    "Order processed: key=%d, custKey=%d, isValid=%b",
                    orderKey, custKey, isValid
            ));
        }
        else if (event.getType() == UpdateType.DELETE) {
            // 删除就输出 isValid=false
            out.collect(new OrderInfo(
                    orderKey,
                    custKey,
                    order.getO_orderdate(),
                    order.getO_shippriority(),
                    false
            ));
            System.out.println("Order deleted: key=" + orderKey);
        }
    }

    @Override
    public void processBroadcastElement(
            CustomerInfo customerInfo,
            Context ctx,
            Collector<OrderInfo> out) throws Exception {

        // 只更新 broadcast state，不再发任何 OrderInfo
        ctx.getBroadcastState(customerStateDesc)
                .put(customerInfo.getCustKey(), customerInfo);

        System.out.println(String.format(
                "Customer broadcast received: key=%d, segment=%s, isValid=%b",
                customerInfo.getCustKey(),
                customerInfo.getMktsegment(),
                customerInfo.isValid()
        ));
    }
}
