package org.example.cquirrel.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.cquirrel.model.DataModels.*;
import org.example.cquirrel.util.DateUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 从本地 TPC-H 文件循环读取 customer/orders/lineitem，
 * 并按照 INSERT 事件推送给下游。
 * 对 date 字段直接调用 DateUtil.isBefore/isAfter/formatDate，
 * 已在工具类中做了空值和格式保护。
 */
public class TPCHFileDataSource implements SourceFunction<UpdateEvent<?>> {
    private volatile boolean isRunning = true;
    private final long sleepInterval;
    private final String customerFilePath;
    private final String ordersFilePath;
    private final String lineItemFilePath;
    private final int maxRecords;

    // 订单和行项目的 cutoff
    private static final String ORDER_CUTOFF = "1995-03-15";
    private static final String SHIP_CUTOFF  = "1995-03-15";

    public TPCHFileDataSource(
            String customerFilePath,
            String ordersFilePath,
            String lineItemFilePath,
            long sleepInterval,
            int maxRecords) {
        this.customerFilePath = customerFilePath;
        this.ordersFilePath   = ordersFilePath;
        this.lineItemFilePath = lineItemFilePath;
        this.sleepInterval    = sleepInterval;
        this.maxRecords       = maxRecords;
    }

    @Override
    public void run(SourceContext<UpdateEvent<?>> ctx) throws Exception {
        System.out.println("\n===== READING DATA FROM TPC-H FILES =====");
        System.out.println("Customer file: " + customerFilePath);
        System.out.println("Orders file:   " + ordersFilePath);
        System.out.println("LineItem file: " + lineItemFilePath);
        System.out.println("Max records:   " + maxRecords);

        List<Customer> customers = loadCustomers();
        System.out.println("Loaded " + customers.size() + " customers");
        List<Orders> orders     = loadOrders();
        System.out.println("Loaded " + orders.size()   + " orders");
        List<LineItem> items    = loadLineItems();
        System.out.println("Loaded " + items.size()    + " line items");

        // 1) 发送 customer
        System.out.println("\n----- SENDING CUSTOMERS -----");
        for (Customer c : customers) {
            if (!isRunning) break;
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "customer", c));
            if ("BUILDING".equals(c.getC_mktsegment())) {
                System.out.println("Sent BUILDING customer: " +
                        c.getC_custkey() + " – " + c.getC_name());
            }
            Thread.sleep(sleepInterval);
        }

        // 2) 发送 orders
        System.out.println("\n----- SENDING ORDERS -----");
        for (Orders o : orders) {
            if (!isRunning) break;
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "orders", o));
            if (DateUtil.isBefore(o.getO_orderdate(), ORDER_CUTOFF)) {
                System.out.println("Sent order before cutoff: " +
                        o.getO_orderkey() +
                        " (Date: " + DateUtil.formatDate(o.getO_orderdate()) + ")");
            }
            Thread.sleep(sleepInterval);
        }

        // 3) 发送 lineitem
        System.out.println("\n----- SENDING LINEITEMS -----");
        for (LineItem li : items) {
            if (!isRunning) break;
            ctx.collect(new UpdateEvent<>(UpdateType.INSERT, "lineitem", li));
            if (DateUtil.isAfter(li.getL_shipdate(), SHIP_CUTOFF)) {
                System.out.println("Sent lineitem after cutoff: Order " +
                        li.getL_orderkey() + " Line " + li.getL_linenumber() +
                        " (Ship date: " + DateUtil.formatDate(li.getL_shipdate()) + ")");
            }
            Thread.sleep(sleepInterval);
        }

        System.out.println("===== ALL DATA SENT =====\n");
        // 再等一会儿，防止 downstream 还在处理
        if (isRunning) {
            Thread.sleep(60_000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    // —— 数据加载 —— //

    private boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    private List<Customer> loadCustomers() throws IOException {
        List<Customer> list = new ArrayList<>();
        int count = 0, skipped = 0, lineNo = 0;
        try (BufferedReader r = new BufferedReader(new FileReader(customerFilePath))) {
            String line;
            while ((line = r.readLine()) != null && count < maxRecords) {
                lineNo++;
                String[] f = line.split("\\|");
                if (f.length < 8) { skipped++; continue; }
                try {
                    if (isEmpty(f[0]) || isEmpty(f[3]) || isEmpty(f[5])) {
                        skipped++; continue;
                    }
                    Customer c = new Customer(
                            Integer.parseInt(f[0].trim()),
                            f[1].trim(),
                            f[2].trim(),
                            Integer.parseInt(f[3].trim()),
                            f[4].trim(),
                            Double.parseDouble(f[5].trim()),
                            f[6].trim(),
                            f[7].trim()
                    );
                    list.add(c);
                    count++;
                } catch (Exception ex) {
                    skipped++;
                }
            }
        }
        if (skipped > 0) {
            System.out.println("Skipped " + skipped + " invalid customers");
        }
        return list;
    }

    private List<Orders> loadOrders() throws IOException {
        List<Orders> list = new ArrayList<>();
        int count = 0, skipped = 0, lineNo = 0;
        try (BufferedReader r = new BufferedReader(new FileReader(ordersFilePath))) {
            String line;
            while ((line = r.readLine()) != null && count < maxRecords) {
                lineNo++;
                String[] f = line.split("\\|");
                if (f.length < 9) { skipped++; continue; }
                try {
                    if (isEmpty(f[0]) || isEmpty(f[1]) || isEmpty(f[3]) || isEmpty(f[7])) {
                        skipped++; continue;
                    }
                    Date d = DateUtil.parseDate(f[4]);
                    Orders o = new Orders(
                            Integer.parseInt(f[0].trim()),
                            Integer.parseInt(f[1].trim()),
                            isEmpty(f[2]) ? ' ' : f[2].trim().charAt(0),
                            Double.parseDouble(f[3].trim()),
                            d,
                            f[5].trim(),
                            f[6].trim(),
                            Integer.parseInt(f[7].trim()),
                            f[8].trim()
                    );
                    list.add(o);
                    count++;
                } catch (Exception ex) {
                    skipped++;
                }
            }
        }
        if (skipped > 0) {
            System.out.println("Skipped " + skipped + " invalid orders");
        }
        return list;
    }

    private List<LineItem> loadLineItems() throws IOException {
        List<LineItem> list = new ArrayList<>();
        int count = 0, skipped = 0, lineNo = 0;
        try (BufferedReader r = new BufferedReader(new FileReader(lineItemFilePath))) {
            String line;
            while ((line = r.readLine()) != null && count < maxRecords) {
                lineNo++;
                String[] f = line.split("\\|");
                if (f.length < 16) { skipped++; continue; }
                try {
                    if (isEmpty(f[0]) || isEmpty(f[1]) || isEmpty(f[2]) ||
                            isEmpty(f[3]) || isEmpty(f[4]) || isEmpty(f[5]) ||
                            isEmpty(f[6]) || isEmpty(f[7])) {
                        skipped++; continue;
                    }
                    Date ship   = DateUtil.parseDate(f[10]);
                    Date commit = DateUtil.parseDate(f[11]);
                    Date recv   = DateUtil.parseDate(f[12]);
                    LineItem li = new LineItem(
                            Integer.parseInt(f[0].trim()),
                            Integer.parseInt(f[3].trim()),
                            Integer.parseInt(f[1].trim()),
                            Integer.parseInt(f[2].trim()),
                            Integer.parseInt(f[4].trim()),
                            Double.parseDouble(f[5].trim()),
                            Double.parseDouble(f[6].trim()),
                            Double.parseDouble(f[7].trim()),
                            isEmpty(f[8]) ? ' ' : f[8].trim().charAt(0),
                            isEmpty(f[9]) ? ' ' : f[9].trim().charAt(0),
                            ship, commit, recv,
                            f[13].trim(),
                            f[14].trim(),
                            f[15].trim()
                    );
                    list.add(li);
                    count++;
                } catch (Exception ex) {
                    skipped++;
                }
            }
        }
        if (skipped > 0) {
            System.out.println("Skipped " + skipped + " invalid lineitems");
        }
        return list;
    }
}