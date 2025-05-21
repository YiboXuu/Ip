package org.example.cquirrel.model;

import java.io.Serializable;
import java.util.Date;

// 存储Revenue更新，用于聚合
public class RevenueUpdate implements Serializable {
    private final int orderKey;
    private final double deltaRevenue;
    private final Date orderDate;
    private final int shipPriority;

    public RevenueUpdate(int orderKey, double deltaRevenue, Date orderDate, int shipPriority) {
        this.orderKey = orderKey;
        this.deltaRevenue = deltaRevenue;
        this.orderDate = orderDate;
        this.shipPriority = shipPriority;
    }

    public int getOrderKey() {
        return orderKey;
    }

    public double getDeltaRevenue() {
        return deltaRevenue;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public int getShipPriority() {
        return shipPriority;
    }

    @Override
    public String toString() {
        return "RevenueUpdate{" +
                "orderKey=" + orderKey +
                ", deltaRevenue=" + deltaRevenue +
                ", orderDate=" + orderDate +
                ", shipPriority=" + shipPriority +
                '}';
    }
}