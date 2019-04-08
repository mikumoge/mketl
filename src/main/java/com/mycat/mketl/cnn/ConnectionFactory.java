package com.mycat.mketl.cnn;


import com.mycat.mketl.core.Configuration;

import java.util.Map;
import java.util.Set;

public class ConnectionFactory {
    public static Connection[] createConnection(Configuration conf){
        return getConnections(conf);
    }

    // 解析所有的连接对象---每一个连接对象都有唯一的job编号
    private static Connection[] getConnections(Configuration conf){
        Map<String,String> cnnsMap = conf.getConnectionConfig();
        Connection[] cnns=new Connection[cnnsMap.size()];
        Set<String> keySet = cnnsMap.keySet();
        int index=0;
        for (String key : keySet) {
            String[] cnnInfos = cnnsMap.get(key).split("\\$");
            Connection cnn=new Connection(key,cnnInfos[0],cnnInfos[1],cnnInfos[2],cnnInfos[3]);
            cnns[index]=cnn;
            index++;
        }
        return cnns;
    }
}
