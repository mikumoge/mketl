package com.mycat.mketl.cnn;

import com.mycat.mketl.tbl.Admin;
import com.mycat.mketl.tbl.RDBAdmin;
import org.apache.log4j.Logger;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Connection {
    private String driver;
    private String sourceType=null;
    private String sinkType=null;
    private String[] tableNames;
    private String sourceDbType=null;
    private String sinkDbType=null;
    private String cid;
    private Logger LOG=Logger.getLogger(getClass());
    private static Properties sourceProps=new Properties();
    private static Properties sinkProps=new Properties();

    public java.sql.Connection newConnection(int type) {
        try {
            if(type==0){
                return DriverManager.getConnection(sourceProps.getProperty("url"),sourceProps.getProperty("user"),sourceProps.getProperty("password"));
            }
            return DriverManager.getConnection(sinkProps.getProperty("url"),sinkProps.getProperty("user"),sinkProps.getProperty("password"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Connection(){

    }

    public String getConnectionSourceType(){
        return sourceType;
    }
    public String getConnectionSinkType(){
        return sinkType;
    }

    public void init(String url){
        if(url.startsWith("jdbc:mysql")){
            if(sourceType==null){
                sourceType="MYSQL";
                sourceDbType="RDB";
                sinkDbType="RDB";
            }else{
                sinkType="MYSQL";
                sinkDbType="RDB";
            }
            driver="com.mysql.jdbc.Driver";
        }
        try {
//            if(driver!=null&&!sinkType.equals(sourceType)){
//                Class.forName(driver);
//            }
              Class.forName(driver);
        } catch (ClassNotFoundException e) {
            LOG.error("Load class "+driver+" failed.",e);
        }
    }

    public Connection(String cid,String urlsStr, String usersStr, String passwordsStr, String tableNamesStr) {
        this.cid=cid;
        String[] urls=urlsStr.split("@");
        String[] users=usersStr.split("@");
        String[] passwords=passwordsStr.split("@");
        String[] tableNames=tableNamesStr.split("@");
        init(urls[0]);
        sourceProps.clear();
        sinkProps.clear();
        sourceProps.setProperty("url",urls[0]);
        sourceProps.setProperty("user",users[0]);
        sourceProps.setProperty("password",passwords[0]);
        sinkProps.setProperty("url",urls[1]);
        sinkProps.setProperty("user",users[1]);
        sinkProps.setProperty("password",passwords[1]);
        this.tableNames=tableNames;
    }

    public Admin getAdmin(){
        if(sourceDbType.equals("RDB")&&sourceDbType.equals(sinkDbType)){
            return new RDBAdmin(tableNames,cid);
        }
        return null;
    }

    public String getCid() {
        return cid;
    }
}
