package com.mycat.mketl.utils;

public class JDBCUtils {
    public static void release(AutoCloseable... closeObjs){
        for (AutoCloseable closeObj : closeObjs) {
            try {
                if (closeObj != null) {
                    closeObj.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
