package org.example.cquirrel.model;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

public class DataModels {

    // 更新操作类型
    public enum UpdateType {
        INSERT, DELETE
    }

    // 基础更新事件
    public static class UpdateEvent<T> implements Serializable {
        private final UpdateType type;
        private final String tableName;
        private final T data;

        public UpdateEvent(UpdateType type, String tableName, T data) {
            this.type = type;
            this.tableName = tableName;
            this.data = data;
        }

        public UpdateType getType() {
            return type;
        }

        public String getTableName() {
            return tableName;
        }

        public T getData() {
            return data;
        }

        @Override
        public String toString() {
            return "UpdateEvent{" +
                    "type=" + type +
                    ", tableName='" + tableName + '\'' +
                    ", data=" + data +
                    '}';
        }
    }

    // 客户表
    public static class Customer implements Serializable {
        private final int c_custkey;
        private final String c_name;
        private final String c_address;
        private final int c_nationkey;
        private final String c_phone;
        private final double c_acctbal;
        private final String c_mktsegment;
        private final String c_comment;

        public Customer(int c_custkey, String c_name, String c_address, int c_nationkey,
                        String c_phone, double c_acctbal, String c_mktsegment, String c_comment) {
            this.c_custkey = c_custkey;
            this.c_name = c_name;
            this.c_address = c_address;
            this.c_nationkey = c_nationkey;
            this.c_phone = c_phone;
            this.c_acctbal = c_acctbal;
            this.c_mktsegment = c_mktsegment;
            this.c_comment = c_comment;
        }

        public int getC_custkey() {
            return c_custkey;
        }

        public String getC_name() {
            return c_name;
        }

        public String getC_address() {
            return c_address;
        }

        public int getC_nationkey() {
            return c_nationkey;
        }

        public String getC_phone() {
            return c_phone;
        }

        public double getC_acctbal() {
            return c_acctbal;
        }

        public String getC_mktsegment() {
            return c_mktsegment;
        }

        public String getC_comment() {
            return c_comment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Customer customer = (Customer) o;
            return c_custkey == customer.c_custkey;
        }

        @Override
        public int hashCode() {
            return Objects.hash(c_custkey);
        }

        @Override
        public String toString() {
            return "Customer{" +
                    "c_custkey=" + c_custkey +
                    ", c_mktsegment='" + c_mktsegment + '\'' +
                    '}';
        }
    }

    // 订单表
    public static class Orders implements Serializable {
        private final int o_orderkey;
        private final int o_custkey;
        private final char o_orderstatus;
        private final double o_totalprice;
        private final Date o_orderdate;
        private final String o_orderpriority;
        private final String o_clerk;
        private final int o_shippriority;
        private final String o_comment;

        public Orders(int o_orderkey, int o_custkey, char o_orderstatus, double o_totalprice,
                      Date o_orderdate, String o_orderpriority, String o_clerk,
                      int o_shippriority, String o_comment) {
            this.o_orderkey = o_orderkey;
            this.o_custkey = o_custkey;
            this.o_orderstatus = o_orderstatus;
            this.o_totalprice = o_totalprice;
            this.o_orderdate = o_orderdate;
            this.o_orderpriority = o_orderpriority;
            this.o_clerk = o_clerk;
            this.o_shippriority = o_shippriority;
            this.o_comment = o_comment;
        }

        public int getO_orderkey() {
            return o_orderkey;
        }

        public int getO_custkey() {
            return o_custkey;
        }

        public char getO_orderstatus() {
            return o_orderstatus;
        }

        public double getO_totalprice() {
            return o_totalprice;
        }

        public Date getO_orderdate() {
            return o_orderdate;
        }

        public String getO_orderpriority() {
            return o_orderpriority;
        }

        public String getO_clerk() {
            return o_clerk;
        }

        public int getO_shippriority() {
            return o_shippriority;
        }

        public String getO_comment() {
            return o_comment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Orders orders = (Orders) o;
            return o_orderkey == orders.o_orderkey;
        }

        @Override
        public int hashCode() {
            return Objects.hash(o_orderkey);
        }

        @Override
        public String toString() {
            return "Orders{" +
                    "o_orderkey=" + o_orderkey +
                    ", o_custkey=" + o_custkey +
                    ", o_orderdate=" + o_orderdate +
                    ", o_shippriority=" + o_shippriority +
                    '}';
        }
    }

    // 订单项表
    public static class LineItem implements Serializable {
        private final int l_orderkey;
        private final int l_linenumber;
        private final int l_partkey;
        private final int l_suppkey;
        private final int l_quantity;
        private final double l_extendedprice;
        private final double l_discount;
        private final double l_tax;
        private final char l_returnflag;
        private final char l_linestatus;
        private final Date l_shipdate;
        private final Date l_commitdate;
        private final Date l_receiptdate;
        private final String l_shipinstruct;
        private final String l_shipmode;
        private final String l_comment;

        public LineItem(int l_orderkey, int l_linenumber, int l_partkey, int l_suppkey,
                        int l_quantity, double l_extendedprice, double l_discount, double l_tax,
                        char l_returnflag, char l_linestatus, Date l_shipdate, Date l_commitdate,
                        Date l_receiptdate, String l_shipinstruct, String l_shipmode, String l_comment) {
            this.l_orderkey = l_orderkey;
            this.l_linenumber = l_linenumber;
            this.l_partkey = l_partkey;
            this.l_suppkey = l_suppkey;
            this.l_quantity = l_quantity;
            this.l_extendedprice = l_extendedprice;
            this.l_discount = l_discount;
            this.l_tax = l_tax;
            this.l_returnflag = l_returnflag;
            this.l_linestatus = l_linestatus;
            this.l_shipdate = l_shipdate;
            this.l_commitdate = l_commitdate;
            this.l_receiptdate = l_receiptdate;
            this.l_shipinstruct = l_shipinstruct;
            this.l_shipmode = l_shipmode;
            this.l_comment = l_comment;
        }

        public int getL_orderkey() {
            return l_orderkey;
        }

        public int getL_linenumber() {
            return l_linenumber;
        }

        public int getL_partkey() {
            return l_partkey;
        }

        public int getL_suppkey() {
            return l_suppkey;
        }

        public int getL_quantity() {
            return l_quantity;
        }

        public double getL_extendedprice() {
            return l_extendedprice;
        }

        public double getL_discount() {
            return l_discount;
        }

        public double getL_tax() {
            return l_tax;
        }

        public char getL_returnflag() {
            return l_returnflag;
        }

        public char getL_linestatus() {
            return l_linestatus;
        }

        public Date getL_shipdate() {
            return l_shipdate;
        }

        public Date getL_commitdate() {
            return l_commitdate;
        }

        public Date getL_receiptdate() {
            return l_receiptdate;
        }

        public String getL_shipinstruct() {
            return l_shipinstruct;
        }

        public String getL_shipmode() {
            return l_shipmode;
        }

        public String getL_comment() {
            return l_comment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LineItem lineItem = (LineItem) o;
            return l_orderkey == lineItem.l_orderkey &&
                    l_linenumber == lineItem.l_linenumber;
        }

        @Override
        public int hashCode() {
            return Objects.hash(l_orderkey, l_linenumber);
        }

        @Override
        public String toString() {
            return "LineItem{" +
                    "l_orderkey=" + l_orderkey +
                    ", l_linenumber=" + l_linenumber +
                    ", l_extendedprice=" + l_extendedprice +
                    ", l_discount=" + l_discount +
                    ", l_shipdate=" + l_shipdate +
                    '}';
        }
    }

    // 查询结果
    public static class QueryResult implements Serializable {
        private final int o_orderkey;
        private final double revenue;
        private final Date o_orderdate;
        private final int o_shippriority;

        public QueryResult(int o_orderkey, double revenue, Date o_orderdate, int o_shippriority) {
            this.o_orderkey = o_orderkey;
            this.revenue = revenue;
            this.o_orderdate = o_orderdate;
            this.o_shippriority = o_shippriority;
        }

        public int getO_orderkey() {
            return o_orderkey;
        }

        public double getRevenue() {
            return revenue;
        }

        // 改成返回 Date 而不是 String
        public Date getO_orderdate() {
            return o_orderdate;
        }

        public int getO_shippriority() {
            return o_shippriority;
        }

        @Override
        public String toString() {
            return "QueryResult{" +
                    "o_orderkey=" + o_orderkey +
                    ", revenue=" + revenue +
                    ", o_orderdate=" + o_orderdate +
                    ", o_shippriority=" + o_shippriority +
                    '}';
        }
    }
}