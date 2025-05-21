package org.example.cquirrel.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 线程安全且“空值/格式错”不抛异常的日期工具。
 */
public class DateUtil {
    private static final ThreadLocal<SimpleDateFormat> FORMATTER =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));

    /**
     * 将字符串解析为 Date。空串或格式不对返回 null。
     */
    public static Date parseDate(String dateStr) {
        if (dateStr == null || dateStr.trim().isEmpty()) {
            return null;
        }
        try {
            return FORMATTER.get().parse(dateStr.trim());
        } catch (ParseException e) {
            // LOG.warn("parseDate failed: {}", dateStr, e);
            return null;
        }
    }

    /**
     * 判断 date 是否严格在 cutoffDateStr 之前：
     * 任意一方为 null 或 cutoff 格式错 => 返回 false。
     */
    public static boolean isBefore(Date date, String cutoffDateStr) {
        if (date == null) {
            return false;
        }
        Date cutoff = parseDate(cutoffDateStr);
        if (cutoff == null) {
            return false;
        }
        return date.before(cutoff);
    }

    /**
     * 判断 date 是否严格在 cutoffDateStr 之后：
     * 任意一方为 null 或 cutoff 格式错 => 返回 false。
     */
    public static boolean isAfter(Date date, String cutoffDateStr) {
        if (date == null) {
            return false;
        }
        Date cutoff = parseDate(cutoffDateStr);
        if (cutoff == null) {
            return false;
        }
        return date.after(cutoff);
    }

    /**
     * 将 Date 格式化为 yyyy-MM-dd；date 为 null 返回 "N/A"。
     */
    public static String formatDate(Date date) {
        if (date == null) {
            return "N/A";
        }
        return FORMATTER.get().format(date);
    }
}