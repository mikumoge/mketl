package com.mycat.mketl;

import com.mycat.mketl.cnn.Connection;
import com.mycat.mketl.cnn.ConnectionFactory;
import com.mycat.mketl.core.Configuration;
import com.mycat.mketl.tbl.Admin;
import com.mycat.mketl.tbl.Field;
import com.mycat.mketl.tbl.Table;
import com.mycat.mketl.utils.UserNameProductUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DemoTest {
    private static Logger LOG=Logger.getLogger(Configuration.class);
    private static long startTime;
    private static CountDownLatch signalThread;
    public static void main(String[] args) throws IOException, SQLException {
        startTime=System.currentTimeMillis();
        // 获取配置对象
        Configuration conf=Configuration.getInstance("t.properties");
        LOG.info("Begin to create job Connection.");
        Connection[] cnns = ConnectionFactory.createConnection(conf);
        ExecutorService service = Executors.newFixedThreadPool(cnns.length);
        signalThread=new CountDownLatch(2);
        for (Connection cnn : cnns) {
            Admin admin = cnn.getAdmin();
            service.submit(new DemoTest().new A(admin));
        }
        service.shutdown();
    }

    class A implements Runnable{
        public Admin admin;
        public A(Admin admin){
            this.admin=admin;
        }

        @Override
        public void run() {
            admin.executeJob();
            signalThread.countDown();
            if(signalThread.getCount()==0){
                long endTime = System.currentTimeMillis();
                System.out.println("任务总耗时："+ (endTime -startTime));
            }
        }
    }

    public static void m1(Admin admin) throws SQLException {
        java.sql.Connection con = admin.getSourceTable().getConnection(0);
        int startId=14810012;
        for (int k = 0; k < 200; k++) {
            StringBuilder preSql=new StringBuilder("insert into stu values(?,?,?)");
            for (int i = 1; i <= 99999; i++) {
                preSql.append(",(?,?,?)");
            }
            PreparedStatement ps = con.prepareStatement(preSql.toString());
            for (int i = 0; i < 100000; i++) {
                ps.setObject(i*3+1,i+startId);
                ps.setObject(i*3+2, UserNameProductUtils.getName());
                ps.setObject(i*3+3,(int)(Math.random()*10+22));
            }
            startId+=100000;
            ps.addBatch();
            ps.executeBatch();
        }
    }
}
