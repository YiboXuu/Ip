package org.example.cquirrel.model;

import java.io.Serializable;
import java.util.Date;

// 存储Orders的必要信息，用于广播
public class OrderInfo implements Serializable {
    private final int orderKey;
    private final int custKey;
    private final Date orderDate;
    private final int shipPriority;
    private final boolean isValid;

    public OrderInfo(int orderKey, int custKey, Date orderDate, int shipPriority, boolean isValid) {
        this.orderKey = orderKey;
        this.custKey = custKey;
        this.orderDate = orderDate;
        this.shipPriority = shipPriority;
        this.isValid = isValid;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public int getCustKey() {
        return custKey;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public int getShipPriority() {
        return shipPriority;
    }

    public boolean isValid() {
        return isValid;
    }

    @Override
    public String toString() {
        return "OrderInfo{" +
                "orderKey=" + orderKey +
                ", custKey=" + custKey +
                ", orderDate=" + orderDate +
                ", shipPriority=" + shipPriority +
                ", isValid=" + isValid +
                '}';
    }
}