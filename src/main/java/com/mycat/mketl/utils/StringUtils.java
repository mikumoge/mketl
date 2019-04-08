package com.mycat.mketl.utils;

public class StringUtils {
    public static String getOrDefault(String orc,String def){
        return orc!=null?orc:def;
    }
    public static String getConcatBeforeOrDefault(String orc,String cont,String def){
        return orc!=null?cont+orc:def;
    }
}
