package org.example.cquirrel.model;

import java.io.Serializable;
import java.util.Date;

// 存储Customer的必要信息，用于广播
public class CustomerInfo implements Serializable {
    private final int custKey;
    private final String mktsegment;
    private final boolean isValid;

    public CustomerInfo(int custKey, String mktsegment, boolean isValid) {
        this.custKey = custKey;
        this.mktsegment = mktsegment;
        this.isValid = isValid;
    }

    public int getCustKey() {
        return custKey;
    }

    public String getMktsegment() {
        return mktsegment;
    }

    public boolean isValid() {
        return isValid;
    }

    @Override
    public String toString() {
        return "CustomerInfo{" +
                "custKey=" + custKey +
                ", mktsegment='" + mktsegment + '\'' +
                ", isValid=" + isValid +
                '}';
    }
}