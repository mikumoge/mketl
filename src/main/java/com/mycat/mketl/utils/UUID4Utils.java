package com.mycat.mketl.utils;

import java.util.Random;

public class UUID4Utils {
    public static String getUuid(int digit){// bit
        String orc="qazwsxedcrfvtgbyhnujmikolpQAZWSXEDCRFVTGBYHNUJMIKOLP1234567890";
        Random rm=new Random();
        StringBuilder result= new StringBuilder();
        while(digit--!=0){
            int index = rm.nextInt(orc.length() - 1);
            result.append(orc.toCharArray()[index]);
        }
        return result.toString();
    }
}
