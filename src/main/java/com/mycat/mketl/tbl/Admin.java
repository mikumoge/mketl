package com.mycat.mketl.tbl;

import com.mycat.mketl.core.Configuration;
import com.mycat.mketl.utils.ArrayUtils;
import com.mycat.mketl.utils.JDBCUtils;
import com.mycat.mketl.utils.StringUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class Admin {
    static {
        starTime=System.currentTimeMillis();
    }
    private static CountDownLatch signalThreadNum;
    private static Logger LOG=Logger.getLogger(Admin.class);
    Table sourceTable=new Table();
    Table sinkTable=new Table();
    public static long starTime;
    protected abstract void parseFields(Configuration conf, String cid);
    public int totalTaskNum=0;

    public Table getSourceTable() {
        return sourceTable;
    }

    public Table getSinkTable(){
        return sinkTable;
    }

    public abstract String getCid();

    // 设置分区查询条件-----wheres  List<String>
    public void setWhereList(Connection sourceCnn){
        String keyFieldName = sourceTable.getKeyFieldName();
        ResultSet rs = null;
        PreparedStatement ps=null;
        try {
            String where= StringUtils.getConcatBeforeOrDefault(sourceTable.getWhereConfig()," where ","");
            String sql="SELECT "+keyFieldName+" FROM " + sourceTable.getTableName()+where+" ORDER BY "+keyFieldName+" ASC";
            List<String> strSet=new ArrayList<>();
            List<Integer> intSet=new ArrayList<>();
            ps = sourceCnn.prepareStatement(sql);
            rs = ps.executeQuery();
            int inp=0;
            while(rs.next()){
                Object val = rs.getObject(1);
                Integer intValue=null;
                String strValue=null;
                if(val instanceof Integer){
                    intValue= (Integer) val;
                } else if(val instanceof String){
                    strValue = (String) val;
                }
                if(intValue==null&&!strSet.contains(strValue)) {
                    if(inp%sourceTable.getSplitNum()==0){
                        strSet.add(strValue);
                    }
                    inp++;
                }else if(intValue!=null&&!intSet.contains(intValue)) {
                    if(inp%sourceTable.getSplitNum()==0){
                        intSet.add(intValue);
                    }
                    inp++;
                }
            }
            List<String> wheres=new ArrayList<>();
            for (int i = 0; i < (intSet.size()==0?strSet.size():intSet.size()); i++) {
                if(intSet.size()!=0){
                    wheres.add(" "+keyFieldName+">="+intSet.get(i)+(i==intSet.size()-1?" ":" and "+keyFieldName+"<"+intSet.get(i+1)));
                }else{
                    wheres.add(" "+keyFieldName+">='"+strSet.get(i)+(i==strSet.size()-1?"":"' and "+keyFieldName+"<'"+strSet.get(i+1))+"'");
                }
            }
            sourceTable.setWheres(wheres);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.release(rs,ps);
        }
    }

    public void executeJob(){
        Connection sourceCnn=sourceTable.getConnection(0);
        Connection sinkCnn = sinkTable.getConnection(1);
        // 对字段列的解析
        String columns= ArrayUtils.array2String(sourceTable.getFieldsExpression(),"*");

        //对where条件的解析
        String whereConfig = sourceTable.getWhereConfig();
        setWhereList(sourceCnn);
        List<String> whereList=sourceTable.getWheres();
        if(sourceTable.getSplitNum()>1){
            whereConfig=(whereConfig==null)?" WHERE 1=1 and ":" WHERE "+whereConfig+" and ";
        }else{
            whereConfig=(whereConfig==null)?" WHERE ":" WHERE "+whereConfig;
        }
        totalTaskNum=(whereList==null?1:whereList.size());
        LOG.info("Total task num is "+totalTaskNum+" of Job"+this.getCid()+".");
        ExecutorService service = Executors.newFixedThreadPool(totalTaskNum);
        signalThreadNum=new CountDownLatch(4);
        Task task;
        for (int i = 0; i < totalTaskNum; i++) {
            task=new Task(i);
            task.setColumns(columns);
            task.setSourceConnection(sourceCnn);
            task.setSinkConnection(sinkCnn);
            task.setCid(this.getCid());
            task.setWhere(whereConfig+(whereList==null?"":whereList.get(task.taskId)));
            service.submit(task);
        }
        service.shutdown();
    }

    protected int getRecordsCount(Connection cnn,String tableName,String where){
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            ps = cnn.prepareStatement("SELECT "+sourceTable.getKeyFieldName()+" FROM "+tableName +" "+ where);
            rs = ps.executeQuery();
            int count=0;
            while(rs.next()){
                count++;
            }
            return count;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JDBCUtils.release(rs,ps);
        }
        return 0;
    }

    class Task implements Runnable{
        private int taskId=0;
        private Connection sourceCnn;
        private Connection sinkCnn;
        private String columns;
        private String where;
        private long startTime=Admin.starTime;
        private String cid;

        public Task(int taskId){
            if(taskId==0)startTime=System.currentTimeMillis();
            this.taskId=taskId;
        }

        @Override
        public void run() {
            executeSplitExport(sourceCnn,sinkCnn,columns,where,taskId);
        }
        public void setColumns(String columns) {
            this.columns=columns;
        }

        public void setWhere(String where) {
            this.where=where;
        }
        // 分区插入 调用次数为split个数
        public void executeSplitExport(Connection sourceCnn,Connection sinkCnn,String columns, String where,int taskId) {
            LOG.info("Begin to execute the "+(taskId+1)+"/"+totalTaskNum+" task of Job "+cid+".");
            String sourceTableName = sourceTable.getTableName();
            Configuration config = Configuration.getConfig();
            PreparedStatement ps=null;
            ResultSet rs=null;
            try {
                // 执行原表的切分查询
                String sql = "SELECT " + columns + " FROM " + sourceTableName + where;
                ps = sourceCnn.prepareStatement(sql);
                //LOG.info("Executing SQL:\n\t<sql>\n \t\t" + sql + "\t\n\t</sql>");
                rs = ps.executeQuery();
                LOG.info("Executed SELECT SQL of Job "+cid+" finished.");
                ResultSetMetaData metaData = rs.getMetaData();
                String[] fieldsName = sourceTable.getFieldsName();
                if (fieldsName == null) {
                    fieldsName = new String[metaData.getColumnCount()];
                    for (int i = 0; i < fieldsName.length; i++) {
                        fieldsName[i] = metaData.getColumnLabel(i + 1);
                    }
                    sourceTable.setFieldsByNames(fieldsName);
                }

                String[] sinkFieldsNames = sinkTable.getFieldsName();
                String sinkColumns = ArrayUtils.array2String(sinkFieldsNames, "*");

                String sql2 = "INSERT INTO " + sinkTable.getTableName() + "(" + sinkColumns + ") VALUES";
                StringBuilder oneRecordSb = new StringBuilder("(");
                for (int i = 0; i < sinkFieldsNames.length; i++) {
                    oneRecordSb.append("?,");
                }
                oneRecordSb = new StringBuilder(oneRecordSb.substring(0, oneRecordSb.length() - 1) + "),");
                StringBuilder batchRecordSb = new StringBuilder();

                // 微批次设置
                int maxBatchSize = Integer.parseInt(config.get("common.batch.export.max.num"));

                // 单次任务总条数统计
                int counts=sourceTable.getSplitNum();
                int rest=0; //最后一次才进行判断
                StringBuilder batchRecordSb2 = new StringBuilder();
                String values2="";
                int times=0;
                if(taskId==totalTaskNum-1){
                    counts = getRecordsCount(sourceCnn, sourceTableName, where);
                    rest=counts>maxBatchSize?(counts%maxBatchSize):0;
                    //最后一次才进行判断
                    for (int i = 0; counts > maxBatchSize && i < rest; i++) {
                        batchRecordSb2.append(oneRecordSb);
                    }
                    values2 = (batchRecordSb2.length()==0)?"":batchRecordSb2.substring(0, batchRecordSb2.length() - 1);
                    times=counts>maxBatchSize?counts/maxBatchSize:1;
                }
                if(counts<=0)return;
                for (int i = 0; i < (counts > maxBatchSize ? maxBatchSize : counts); i++) {
                    batchRecordSb.append(oneRecordSb);
                }

                String values = batchRecordSb.substring(0, batchRecordSb.length() - 1);
                PreparedStatement ps2 = sinkCnn.prepareStatement(sql2 + values);
                int index = 0;
                int t=0;
                while (rs.next()) {
                    if (index != 0 && index % maxBatchSize == 0) {
                        ps2.addBatch();
                        ps2.executeBatch();
                        ps2.clearBatch();
                        t++;
                        if(t==times&&rest>0){
                            ps2 = sinkCnn.prepareStatement(sql2 + values2);
                        }
                    }
                    for (int i = 0; i < fieldsName.length; i++) {
                        String columnTypeName = metaData.getColumnClassName(i + 1);
                        Object obj = rs.getObject(fieldsName[i], Class.forName(columnTypeName));
                        sourceTable.setFieldTypeByName(obj.getClass().getName(), fieldsName[i]);
                        ps2.setObject((index % maxBatchSize) * fieldsName.length + i + 1, obj);
                    }
                    index++;
                }
                ps2.addBatch();
                ps2.executeBatch();
                LOG.info("Finished to execute the "+(taskId+1)+"/"+totalTaskNum+" task of Job "+cid+".");
                signalThreadNum.countDown();
                if(signalThreadNum.getCount()==0){
                    long endTime = System.currentTimeMillis();
                    System.out.println("任务累计耗时："+(endTime -startTime)+" ms.");
                }
            }catch (SQLException | ClassNotFoundException e){
                e.printStackTrace();
            }finally {
                JDBCUtils.release(rs,ps);
            }
        }

        public void setSourceConnection(Connection sourceCnn) {
            this.sourceCnn=sourceCnn;
        }

        public void setSinkConnection(Connection sinkCnn) {
            this.sinkCnn=sinkCnn;
        }

        public void setCid(String cid) {
            this.cid=cid;
        }
    }
}
