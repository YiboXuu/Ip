package org.example.cquirrel.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.cquirrel.model.DataModels.*;
import org.example.cquirrel.util.DateUtil;

import java.util.Date;

public class TPCHTestDataSource implements SourceFunction<UpdateEvent<?>> {
    private volatile boolean isRunning = true;
    private final long sleepInterval;

    public TPCHTestDataSource(long sleepInterval) {
        this.sleepInterval = sleepInterval;
    }

    @Override
    public void run(SourceContext<UpdateEvent<?>> ctx) throws Exception {
        System.out.println("\n===== GENERATING TEST DATA FOR TPC-H Q3 =====");
        System.out.println("Creating test data that will guarantee multiple query results");

        // -------- 客户数据 --------
        // 创建5个BUILDING客户
        Customer[] buildingCustomers = new Customer[] {
                new Customer(1, "Customer One", "123 Main St", 1, "123-456-7890", 1000.0, "BUILDING", "First test customer"),
                new Customer(2, "Customer Two", "456 Oak Ave", 2, "234-567-8901", 2000.0, "BUILDING", "Second test customer"),
                new Customer(3, "Customer Three", "789 Pine Blvd", 3, "345-678-9012", 3000.0, "BUILDING", "Third test customer"),
                new Customer(4, "Customer Four", "101 Maple Dr", 4, "456-789-0123", 4000.0, "BUILDING", "Fourth test customer"),
                new Customer(5, "Customer Five", "202 Cedar Ln", 5, "567-890-1234", 5000.0, "BUILDING", "Fifth test customer")
        };

        // 创建1个非BUILDING客户作为对照
        Customer nonBuildingCustomer = new Customer(6, "Customer Six", "303 Elm St", 6, "678-901-2345", 6000.0, "AUTOMOBILE", "Non-building customer");

        // -------- 订单数据 --------
        // 为每个BUILDING客户创建符合条件的订单（日期早于1995-03-15）
        Date[] validOrderDates = new Date[] {
                DateUtil.parseDate("1995-01-15"),
                DateUtil.parseDate("1995-02-01"),
                DateUtil.parseDate("1995-02-15"),
                DateUtil.parseDate("1995-03-01"),
                DateUtil.parseDate("1995-03-10"),
                DateUtil.parseDate("1995-03-14")
        };

        Orders[] validOrders = new Orders[] {
                new Orders(101, 1, 'O', 1000.0, validOrderDates[0], "1-URGENT", "Clerk#1", 0, "Customer 1 valid order"),
                new Orders(102, 2, 'O', 2000.0, validOrderDates[1], "2-HIGH", "Clerk#2", 1, "Customer 2 valid order"),
                new Orders(103, 3, 'O', 3000.0, validOrderDates[2], "3-MEDIUM", "Clerk#3", 2, "Customer 3 valid order"),
                new Orders(104, 4, 'O', 4000.0, validOrderDates[3], "4-LOW", "Clerk#4", 0, "Customer 4 valid order"),
                new Orders(105, 5, 'O', 5000.0, validOrderDates[4], "5-LOW", "Clerk#5", 1, "Customer 5 valid order"),
                new Orders(106, 1, 'O', 6000.0, validOrderDates[5], "1-URGENT", "Clerk#1", 2, "Customer 1 second valid order")
        };

        // 一些不符合条件的订单（日期晚于1995-03-15）作为对照
        Date invalidOrderDate = DateUtil.parseDate("1995-03-16");
        Orders[] invalidOrders = new Orders[] {
                new Orders(107, 1, 'O', 7000.0, invalidOrderDate, "1-URGENT", "Clerk#1", 0, "Customer 1 invalid order (too late)"),
                new Orders(108, 6, 'O', 8000.0, validOrderDates[0], "2-HIGH", "Clerk#6", 1, "Non-building customer order (valid date)")
        };

        // -------- 行项目数据 --------
        // 为每个符合条件的订单创建符合条件的行项目（发货日期晚于1995-03-15）
        Date[] validShipDates = new Date[] {
                DateUtil.parseDate("1995-03-16"),
                DateUtil.parseDate("1995-04-01"),
                DateUtil.parseDate("1995-04-15"),
                DateUtil.parseDate("1995-05-01"),
                DateUtil.parseDate("1995-05-15"),
                DateUtil.parseDate("1995-06-01")
        };

        LineItem[] validLineItems = new LineItem[] {
                // 第一个价格是500元，折扣0.1，收入为450
                new LineItem(101, 1, 1, 1, 5, 500.0, 0.1, 0.08, 'N', 'O', validShipDates[0],
                        DateUtil.parseDate("1995-05-01"), DateUtil.parseDate("1995-05-10"),
                        "DELIVER IN PERSON", "TRUCK", "Valid lineitem for order 101"),

                // 第二个价格是1000元，折扣0.2，收入为800
                new LineItem(102, 1, 2, 2, 10, 1000.0, 0.2, 0.08, 'N', 'O', validShipDates[1],
                        DateUtil.parseDate("1995-05-05"), DateUtil.parseDate("1995-05-15"),
                        "DELIVER IN PERSON", "MAIL", "Valid lineitem for order 102"),

                // 第三个价格是1500元，折扣0.15，收入为1275
                new LineItem(103, 1, 3, 3, 15, 1500.0, 0.15, 0.08, 'N', 'O', validShipDates[2],
                        DateUtil.parseDate("1995-05-10"), DateUtil.parseDate("1995-05-20"),
                        "TAKE BACK RETURN", "AIR", "Valid lineitem for order 103"),

                // 第四个价格是2000元，折扣0.05，收入为1900
                new LineItem(104, 1, 4, 4, 20, 2000.0, 0.05, 0.08, 'N', 'O', validShipDates[3],
                        DateUtil.parseDate("1995-06-01"), DateUtil.parseDate("1995-06-10"),
                        "NONE", "RAIL", "Valid lineitem for order 104"),

                // 第五个价格是2500元，折扣0.1，收入为2250
                new LineItem(105, 1, 5, 5, 25, 2500.0, 0.1, 0.08, 'N', 'O', validShipDates[4],
                        DateUtil.parseDate("1995-06-05"), DateUtil.parseDate("1995-06-15"),
                        "DELIVER IN PERSON", "SHIP", "Valid lineitem for order 105"),

                // 第六个价格是3000元，折扣0.25，收入为2250
                new LineItem(106, 1, 6, 1, 30, 3000.0, 0.25, 0.08, 'N', 'O', validShipDates[5],
                        DateUtil.parseDate("1995-07-01"), DateUtil.parseDate("1995-07-10"),
                        "TAKE BACK RETURN", "TRUCK", "Valid lineitem for order 106")
        };

        // 一些不符合条件的行项目（发货日期早于1995-03-15）作为对照
        Date invalidShipDate = DateUtil.parseDate("1995-03-14");
        LineItem[] invalidLineItems = new LineItem[] {
                new LineItem(101, 2, 7, 1, 5, 500.0, 0.1, 0.08, 'N', 'O', invalidShipDate,
                        DateUtil.parseDate("1995-02-01"), DateUtil.parseDate("1995-02-10"),
                        "DELIVER IN PERSON", "TRUCK", "Invalid lineitem for order 101 (too early)"),

                new LineItem(107, 1, 8, 2, 10, 1000.0, 0.2, 0.08, 'N', 'O', validShipDates[0],
                        DateUtil.parseDate("1995-04-01"), DateUtil.parseDate("1995-04-10"),
                        "DELIVER IN PERSON", "MAIL", "Valid shipdate but invalid order date")
        };

        // ---------- 发送所有测试数据 ----------

        // 1. 发送客户数据
        System.out.println("\n----- SENDING CUSTOMER DATA -----");
        for (Customer customer : buildingCustomers) {
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "customer", customer));
            System.out.println("Sent BUILDING customer: " + customer.getC_custkey() + " - " + customer.getC_name());
            Thread.sleep(sleepInterval);
        }

        ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "customer", nonBuildingCustomer));
        System.out.println("Sent NON-BUILDING customer: " + nonBuildingCustomer.getC_custkey() +
                " - " + nonBuildingCustomer.getC_name() + " (" + nonBuildingCustomer.getC_mktsegment() + ")");
        Thread.sleep(sleepInterval);

        // 2. 发送有效订单数据
        System.out.println("\n----- SENDING VALID ORDERS DATA -----");
        for (Orders order : validOrders) {
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "orders", order));
            System.out.println("Sent order before cutoff: " + order.getO_orderkey() +
                    " for customer " + order.getO_custkey() +
                    " (Date: " + DateUtil.formatDate(order.getO_orderdate()) + ")");
            Thread.sleep(sleepInterval);
        }

        // 3. 发送无效订单数据
        System.out.println("\n----- SENDING INVALID ORDERS DATA -----");
        for (Orders order : invalidOrders) {
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "orders", order));
            System.out.println("Sent order (invalid): " + order.getO_orderkey() +
                    " for customer " + order.getO_custkey() +
                    " (Date: " + DateUtil.formatDate(order.getO_orderdate()) + ")");
            Thread.sleep(sleepInterval);
        }

        // 4. 发送有效行项目数据
        System.out.println("\n----- SENDING VALID LINEITEM DATA -----");
        for (LineItem lineItem : validLineItems) {
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "lineitem", lineItem));
            System.out.println("Sent lineitem after cutoff: Order " + lineItem.getL_orderkey() +
                    " Line " + lineItem.getL_linenumber() +
                    " (Ship date: " + DateUtil.formatDate(lineItem.getL_shipdate()) + ")");
            Thread.sleep(sleepInterval);
        }

        // 5. 发送无效行项目数据
        System.out.println("\n----- SENDING INVALID LINEITEM DATA -----");
        for (LineItem lineItem : invalidLineItems) {
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "lineitem", lineItem));
            System.out.println("Sent lineitem (invalid): Order " + lineItem.getL_orderkey() +
                    " Line " + lineItem.getL_linenumber() +
                    " (Ship date: " + DateUtil.formatDate(lineItem.getL_shipdate()) + ")");
            Thread.sleep(sleepInterval);
        }

        System.out.println("\n===== COMPLETED SENDING TEST DATA =====");
        System.out.println("Expected results: 6 orders should produce Q3 results");
        System.out.println("Order 101: revenue = 500.0 * (1 - 0.1) = 450.0");
        System.out.println("Order 102: revenue = 1000.0 * (1 - 0.2) = 800.0");
        System.out.println("Order 103: revenue = 1500.0 * (1 - 0.15) = 1275.0");
        System.out.println("Order 104: revenue = 2000.0 * (1 - 0.05) = 1900.0");
        System.out.println("Order 105: revenue = 2500.0 * (1 - 0.1) = 2250.0");
        System.out.println("Order 106: revenue = 3000.0 * (1 - 0.25) = 2250.0");

        // 等待处理完成
        Thread.sleep(10000);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}