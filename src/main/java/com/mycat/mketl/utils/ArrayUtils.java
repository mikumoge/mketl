package com.mycat.mketl.utils;

public class ArrayUtils {
    public static String array2String(String[] ts,String def){
        if(ts==null) return def;
        String result="";
        for (int i = 0; i < ts.length; i++) {
            result+=ts[i].concat(",");
        }
        return result.substring(0,result.length()-1);
    }
    public static String array2String(String[] ts){
        String result="";
        for (int i = 0; i < ts.length; i++) {
            result+=ts[i].concat(",");
        }
        return result.substring(0,result.length()-1);
    }
}
