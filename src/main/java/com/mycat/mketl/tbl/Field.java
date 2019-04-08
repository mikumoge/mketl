package com.mycat.mketl.tbl;

public class Field {
    private String selectExpression;
    private String fieldName;
    private String fieldType;
    private boolean isKey;

    public boolean isKey() {
        return isKey;
    }

    public String getFieldName() {
        String[] fs = selectExpression.split("\\s+");
        this.fieldName=fs.length>1?fs[1]:selectExpression;
        return fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getSelectExpression() {
        return selectExpression;
    }

    public void setSelectExpression(String selectExpression) {
        this.selectExpression = selectExpression;
    }

    @Override
    public String toString() {
        return "Field{" +
                "selectExpression='" + selectExpression + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", fieldType='" + fieldType + '\'' +
                ", isKey=" + isKey +
                '}';
    }

    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }
}
