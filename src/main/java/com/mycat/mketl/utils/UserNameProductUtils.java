package com.mycat.mketl.utils;

import java.util.Random;

public class UserNameProductUtils {
    public static String getName(){
        System.out.println("=================");
        String orc="qazwsxedcrfvtgbyhnujmiklopQAZWSXEDCRFVTGBYHNUJMIKLOP";
        Random m1 = new Random();
        int size = m1.nextInt(10);
        while(size<3){
            System.out.println(size);
            size = m1.nextInt(10);
        }
        System.out.println("size:"+size);
        Random chm = new Random();
        String result="";
        for (int i = 0; i < size; i++) {
            int index = chm.nextInt(orc.length());
            result+=orc.toCharArray()[index];
        }
        System.out.println(result);
        return result;
    }
}
