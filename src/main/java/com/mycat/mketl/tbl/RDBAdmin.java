package com.mycat.mketl.tbl;

import com.mycat.mketl.core.Configuration;

import java.sql.Connection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RDBAdmin extends Admin{

    public String cid=null;

    public RDBAdmin(String[] tableNames,String cid) {
        sourceTable.setName(tableNames[0]);
        sinkTable.setName(tableNames[1]);
        this.cid=cid;
        parseFields(Configuration.getConfig(),cid);
    }

    @Override
    protected void parseFields(Configuration conf,String cid) {
        String channelColumnsStr = conf.getCommonConfig(cid,"columns");
        Matcher m=null;
        String whereConfig = conf.getCommonConfig(cid,"where");
        String splitsConfig = conf.getCommonConfig(cid,"splits");
        if(channelColumnsStr==null){
            String queryConfig = conf.getCommonConfig(cid,"query");
            Pattern pattern = Pattern.compile("SELECT\\s+(.*)\\s+FROM\\s+\\w*\\s*(WHERE\\s+(.*)?)?");
            m = pattern.matcher(queryConfig.toUpperCase());
            if(m.matches()){
                channelColumnsStr=m.group(1).toLowerCase();
                whereConfig=m.group(3)==null?null:m.group(3).toLowerCase();
            }
        }
        sourceTable.setSplitNum(Integer.parseInt(splitsConfig));
        sourceTable.setWhereConfig(whereConfig);
        assert channelColumnsStr != null;
        if(channelColumnsStr.equals("*")){
            sourceTable.setSelectByStar(true);
            return;
        }
        String[] channelColumns = channelColumnsStr.split("\\$");
        Field[] sourceFields=new Field[channelColumns.length];
        Field[] sinkFields=new Field[channelColumns.length];
        for (int i = 0; i < channelColumns.length; i++) {
            boolean isKey=channelColumns[i].endsWith("#");
            String[] channelColumn = (isKey?channelColumns[i].substring(0,channelColumns[i].length()-1):channelColumns[i]).split("@");
            Field sourceField=new Field();
            Field sinkField=new Field();

            if(channelColumn.length>1){
                sinkField.setSelectExpression(channelColumn[1]);
            }else{
                sinkField.setSelectExpression(channelColumn[0]);
            }
            sourceField.setSelectExpression(channelColumn[0]);
            sourceField.setIsKey(isKey);
            sinkField.setIsKey(isKey);

            sourceFields[i]=sourceField;
            sinkFields[i]=sinkField;
        }
        sourceTable.setFields(sourceFields);
        sinkTable.setFields(sinkFields);
    }

    @Override
    public String getCid() {
        return cid;
    }
}
