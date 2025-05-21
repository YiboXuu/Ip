package org.example.cquirrel;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.cquirrel.model.CustomerInfo;
import org.example.cquirrel.model.DataModels.*;
import org.example.cquirrel.model.OrderInfo;
import org.example.cquirrel.model.RevenueUpdate;
import org.example.cquirrel.processor.DynamicTopNProcessFunction;
import org.example.cquirrel.processor.LineItemProcessor;
import org.example.cquirrel.processor.OrdersProcessor;
import org.example.cquirrel.processor.RevenueAggregator;
import org.example.cquirrel.source.TPCHFileDataSource;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TPCHQ3Application {
    public static void main(String[] args) throws Exception {
        // 参数解析
        String segment      = args.length > 0 ? args[0] : "BUILDING";
        String date         = args.length > 1 ? args[1] : "1995-03-15";
        String customerPath = args.length > 2 ? args[2] : "D:\\Desktop\\it2025spring\\ip\\TPC-H V3.0.1\\dbgen\\customer.tbl";
        String ordersPath   = args.length > 3 ? args[3] : "D:\\Desktop\\it2025spring\\ip\\TPC-H V3.0.1\\dbgen\\orders.tbl";
        String lineItemPath = args.length > 4 ? args[4] : "D:\\Desktop\\it2025spring\\ip\\TPC-H V3.0.1\\dbgen\\lineitem.tbl";
       // String customerPath = args.length > 2 ? args[2] : "/mnt/d/Desktop/it2025spring/ip/TPC-H V3.0.1/dbgen/customer.tbl";
//        String ordersPath = args.length > 3 ? args[3] : "/mnt/d/Desktop/it2025spring/ip/TPC-H V3.0.1/dbgen/orders.tbl";
//        String lineItemPath = args.length > 4 ? args[4] : "/mnt/d/Desktop/it2025spring/ip/TPC-H V3.0.1/dbgen/lineitem.tbl";


        int maxRecords = args.length > 5 ? Integer.parseInt(args[5]) : Integer.MAX_VALUE;

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        env.getCheckpointConfig().disableCheckpointing();

        // 数据源
        DataStream<UpdateEvent<?>> inputStream = env
                .addSource(new TPCHFileDataSource(customerPath, ordersPath, lineItemPath, 0, maxRecords))
                .name("TPCHFileDataSource");

        // 拆分三个流
        DataStream<Customer> customerStream = inputStream
                .filter(e -> "customer".equals(e.getTableName()))
                .map((MapFunction<UpdateEvent<?>, Customer>) e -> (Customer) e.getData())
                .returns(TypeInformation.of(Customer.class))
                .name("CustomerStream");

        DataStream<UpdateEvent<Orders>> ordersStream = inputStream
                .filter(e -> "orders".equals(e.getTableName()))
                .map((MapFunction<UpdateEvent<?>, UpdateEvent<Orders>>) e ->
                        new UpdateEvent<>(e.getType(), e.getTableName(), (Orders) e.getData()))
                .returns(TypeInformation.of(new TypeHint<UpdateEvent<Orders>>() {}))
                .name("OrdersStream");

        DataStream<UpdateEvent<LineItem>> lineItemStream = inputStream
                .filter(e -> "lineitem".equals(e.getTableName()))
                .map((MapFunction<UpdateEvent<?>, UpdateEvent<LineItem>>) e ->
                        new UpdateEvent<>(e.getType(), e.getTableName(), (LineItem) e.getData()))
                .returns(TypeInformation.of(new TypeHint<UpdateEvent<LineItem>>() {}))
                .name("LineItemStream");

        // CustomerInfo & broadcast
        MapStateDescriptor<Integer, CustomerInfo> custDesc = new MapStateDescriptor<>(
                "CustomerBroadcastState",
                BasicTypeInfo.INT_TYPE_INFO,
                TypeInformation.of(CustomerInfo.class));
        MapStateDescriptor<Integer, List<Integer>> custToOrdersDesc = new MapStateDescriptor<>(
                "CustToOrdersBroadcastState",
                BasicTypeInfo.INT_TYPE_INFO,
                TypeInformation.of(new TypeHint<List<Integer>>() {}));
        BroadcastStream<CustomerInfo> customerBroadcast = customerStream
                .map((MapFunction<Customer, CustomerInfo>) c ->
                        new CustomerInfo(c.getC_custkey(), c.getC_mktsegment(),
                                segment.equals(c.getC_mktsegment())))
                .name("CustomerInfoMapper")
                .broadcast(custDesc, custToOrdersDesc);

        // OrdersProcessor & broadcast
        SingleOutputStreamOperator<OrderInfo> orderInfoStream = ordersStream
                .keyBy(e -> e.getData().getO_orderkey())
                .connect(customerBroadcast)
                .process(new OrdersProcessor(date))
                .name("OrdersProcessor");
        MapStateDescriptor<Integer, OrderInfo> orderDesc = new MapStateDescriptor<>(
                "OrderBroadcastState",
                BasicTypeInfo.INT_TYPE_INFO,
                TypeInformation.of(OrderInfo.class));
        MapStateDescriptor<Integer, List<String>> orderToItemsDesc = new MapStateDescriptor<>(
                "OrderToLineItemsBroadcastState",
                BasicTypeInfo.INT_TYPE_INFO,
                TypeInformation.of(new TypeHint<List<String>>() {}));
        BroadcastStream<OrderInfo> orderBroadcast = orderInfoStream
                .broadcast(orderDesc, orderToItemsDesc);

        // LineItemProcessor → RevenueUpdate
        SingleOutputStreamOperator<RevenueUpdate> revenueStream = lineItemStream
                .keyBy(e -> e.getData().getL_orderkey())
                .connect(orderBroadcast)
                .process(new LineItemProcessor(date))
                .name("LineItemProcessor");

        // RevenueAggregator → QueryResult
        DataStream<QueryResult> resultStream = revenueStream
                .keyBy(RevenueUpdate::getOrderKey)
                .process(new RevenueAggregator())
                .name("RevenueAggregator");

        // Dynamic Top-10
        DataStream<List<QueryResult>> top10Stream = resultStream
                .keyBy(r -> "ALL")
                .process(new DynamicTopNProcessFunction(10))
                .setParallelism(1)
                .name("DynamicTop10");

        // 使用自定义 Sink 只输出最终结果
        top10Stream
                .addSink(new FinalTopNSink())
                .setParallelism(1)
                .name("FinalResultSink");

        env.execute("TPC-H Query 3 Final Top-10 (segment=" + segment + ", date=" + date + ")");
    }

    /**
     * 自定义 Sink，仅在作业结束时输出最终的 Top-10 结果
     */

    public static class FinalTopNSink extends RichSinkFunction<List<QueryResult>> {
        private List<QueryResult> lastTopN;
        private transient SimpleDateFormat dateFormatter;

        @Override
        public void open(Configuration parameters) {
            // 初始化日期格式化器，使用完整的年月日格式
            dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public void invoke(List<QueryResult> value, Context context) {
            // 只保存最后一次收到的 Top-N 列表，不做任何输出
            this.lastTopN = value;
        }

        @Override
        public void close() throws IOException {
            // 仅在作业结束时输出结果
            if (lastTopN != null) {
                System.out.println("\n\n========== FINAL TOP-10 RESULTS ==========");
                for (QueryResult qr : lastTopN) {
                    System.out.printf(
                            "Order Key: %-8d | Revenue: $%-12.2f | Order Date: %-10s | Ship Priority: %d\n",
                            qr.getO_orderkey(),
                            qr.getRevenue(),
                            qr.getO_orderdate() != null ?
                                    dateFormatter.format(qr.getO_orderdate()) : "N/A",
                            qr.getO_shippriority()
                    );
                }

                // 同时输出到文件
                String filename = "output/final_top10_" +
                        new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";

                FileWriter writer = new FileWriter(filename);
                writer.write("========== FINAL TOP-10 RESULTS ==========\n");
                for (QueryResult qr : lastTopN) {
                    writer.write(String.format(
                            "Order Key: %-8d | Revenue: $%-12.2f | Order Date: %-10s | Ship Priority: %d\n",
                            qr.getO_orderkey(),
                            qr.getRevenue(),
                            qr.getO_orderdate() != null ?
                                    dateFormatter.format(qr.getO_orderdate()) : "N/A",
                            qr.getO_shippriority()
                    ));
                }
                writer.close();

                System.out.println("\nFinal results have been saved to: " + filename);
            }
        }
    }
}