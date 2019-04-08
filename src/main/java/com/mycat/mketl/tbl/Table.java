package com.mycat.mketl.tbl;

import com.mycat.mketl.utils.UUID4Utils;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;

public class Table {
    private LinkedList<Connection> connection;
    private String tableName;
    private Field[] fields;
    private String whereConfig;
    private List<String> wheres;
    private boolean isSelectByStar;
    private int splitNum=1;

    public void setName(String tableName) {
        this.tableName=tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public Connection getConnection(int type) {
        return new com.mycat.mketl.cnn.Connection().newConnection(type);
    }

    public Field[] getFields() {
        return fields;
    }

    public void setFields(Field[] fields) {
        this.fields = fields;
    }
    public String[] getFieldsExpression(){
        if(fields==null)return null;
        String[] names=new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            names[i]=fields[i].getSelectExpression();
        }
        return names;
    }
    public String[] getFieldsName(){
        if(fields==null) return null;
        String[] names=new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            names[i]=fields[i].getFieldName();
        }
        return names;
    }

    public Field getFieldByName(String fieldName){
        for (Field field : getFields()) {
            if(field.getFieldName().equals(fieldName)){
                return field;
            }
        }
        return null;
    }

    public void setFieldsByNames(String[] fieldNames){
        Field[] fs=new Field[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            Field f=new Field();
            f.setSelectExpression(fieldNames[i]);
            fs[i]=f;
        }
        this.fields=fs;
    }

    public void setFieldTypeByName(String fieldType,String fieldName) {
        for (String fName : getFieldsName()) {
            if(fName.equals(fieldName)){
                getFieldByName(fName).setFieldType(fieldType);
                break;
            }
        }
    }

    public String getKeyFieldName(){
        String keysName="";
        for (Field field : this.fields) {
            if(field.isKey()){
                keysName = ((keysName.length()<=0)?field.getFieldName():keysName+","+field.getFieldName());
            }
        }
        keysName="CONCAT('"+ UUID4Utils.getUuid(4)+"',"+keysName+")";
        return keysName;
    }

    public int getSplitNum(){
        return splitNum;
    }

    public void setWhereConfig(String whereConfig) {
        this.whereConfig=whereConfig;
    }

    public String getWhereConfig() {
        return whereConfig;
    }

    public boolean isSelectByStar() {
        return isSelectByStar;
    }

    public void setSelectByStar(boolean selectByStar) {
        isSelectByStar = selectByStar;
    }

    public void setSplitNum(int splitNum) {
        this.splitNum = splitNum;
    }
    public List<String> getWheres(){
        return wheres;
    }

    public void setWheres(List<String> wheres) {
        this.wheres = wheres;
    }
}
