package demo;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wangcr on 2017/1/20.
 */

class SourceReader implements Runnable {
    private static final int RETRY_TIME = 3;
    private long min;
    private long max;
    private String rawSql;
    private boolean hasCondition;
    private String splitCol;
    private TableDef tableDef;
    private TableTransJob transJob;
    private Connection conn;
    private Connection targetConn;
    private int tableIndex;
    private SimpleConPool srcConnPool;

    public SourceReader(String sql, boolean hasCondition, String splitCol, long min, long max,
                        TableDef tableDef, TableTransJob transJob, SimpleConPool srcConnPool, int index) {
        this.rawSql = sql;
        this.hasCondition = hasCondition;
        this.min = min;
        this.max = max;
        this.srcConnPool = srcConnPool;
        this.splitCol = splitCol;
        this.tableDef = tableDef;
        this.transJob = transJob;
        this.tableIndex = index;
        this.targetConn = null;
        try {
            this.conn = srcConnPool.getConnection();
        } catch (SQLException e) { e.printStackTrace(); }
    }

    public String getRunSql() {
        StringBuilder sb = new StringBuilder(rawSql);
        String runSql;
        if (hasCondition) {
            sb.append(" AND ");
        } else {
            sb.append(" WHERE ");
        }
        sb.append(splitCol + " BETWEEN " + min + " AND " + max);
        runSql = sb.toString();
        // System.out.println(runSql);
        return runSql;
    }

    private int daysBetween() {
        int days = 0;

        return days;
    }

    public void transData(String fetchSql, String targetSql) {
        int retry = 0;
        for (retry = 0; retry < RETRY_TIME; retry++) {
            Statement stmt = null;
            ResultSet rs = null;
            long beginTime = System.currentTimeMillis();
            try {
                if (conn.isClosed()) {
                    conn = srcConnPool.getConnection();
                    TransJobService.logger.warn(Thread.currentThread().getName() + "Source connection closed by server!");
                    if (conn.isClosed()) {
                        TransJobService.logger.warn(Thread.currentThread().getName() + "Cannot get new connection!");
                        throw new RuntimeException(Thread.currentThread().getName() + "Cannot get new connection!");
                    }
                }
                stmt = conn.createStatement();
                rs = stmt.executeQuery(fetchSql);
            } catch (CommunicationsException e) {
                TransJobService.logger.warn(Thread.currentThread().getName() + ": source connection failure, try to reconnect");
                continue;
            } catch (SQLException e) { e.printStackTrace(); }

                //System.out.println("Fetch data SQL: " + fetchSql);
            try{
                String tableName = rs.getMetaData().getTableName(1);

                if (rs.next()) {
                    long fetchTime = System.currentTimeMillis();
                    TransJobService.logger.info(Thread.currentThread().getName() + ": " + fetchSql + " : FETCH's time cost: " +
                            (fetchTime - beginTime)/1000);
                    targetConn.setAutoCommit(false);
                    PreparedStatement ps;
                    ps = targetConn.prepareStatement(targetSql);

                    rs.beforeFirst();
                    long rowCount = 0;
                    long totalRowCount = 0;
                    int colCount = rs.getMetaData().getColumnCount();
                    int[] colTypes = new int[colCount+1];

                    for (int i = 1; i <= colCount; i++) {
                        colTypes[i] = rs.getMetaData().getColumnType(i);
                    }
                    while (rs.next()) {
                        for (int i = 1; i <= colCount; i++) {
                            if ((colTypes[i] == Types.INTEGER) || (colTypes[i] == Types.SMALLINT) || (colTypes[i] == Types.TINYINT)) {
                                int val = rs.getInt(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setInt(i, val);
                                }
                            } else if (colTypes[i] == Types.BIT) {
                                byte val = rs.getByte(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setByte(i, val);
                                }
                            } else if (colTypes[i] == Types.BIGINT) {
                                long val = rs.getLong(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setLong(i, val);
                                }
                            } else if ((colTypes[i] == Types.VARCHAR) || (colTypes[i] == Types.CHAR) || (colTypes[i] == Types.LONGVARCHAR)) {
                                ps.setString(i, rs.wasNull() ? null : rs.getString(i));
                            } else if (colTypes[i] == Types.DATE) {
                                java.sql.Date val = rs.getDate(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setDate(i, val);
                                }
                            } else if (colTypes[i] == Types.DECIMAL) {
                                BigDecimal val = rs.getBigDecimal(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setBigDecimal(i, val);
                                }
                            } else if (colTypes[i] == Types.DOUBLE) {
                                double val = rs.getDouble(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setDouble(i, val);
                                }
                            } else if (colTypes[i] == Types.FLOAT) {
                                float val = rs.getFloat(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setFloat(i, val);
                                }
                            } else if (colTypes[i] == Types.BOOLEAN) {
                                boolean val = rs.getBoolean(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setBoolean(i, val);
                                }
                            } else if (colTypes[i] == Types.TIMESTAMP) {
                                Timestamp val = rs.getTimestamp(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setTimestamp(i, val);
                                }
                            } else if (colTypes[i] == Types.TIME) {
                                Time val = rs.getTime(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setTime(i, val);
                                }
                            } else if (colTypes[i] == Types.CLOB) {
                                Clob val = rs.getClob(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setClob(i, val);
                                }
                            } else if ((colTypes[i] == Types.BLOB) || (colTypes[i] == Types.BINARY)) {
                                Blob val = rs.getBlob(i);
                                if (rs.wasNull()) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setBlob(i, val);
                                }
                            } else {
                                TransJobService.logger.warn("Unsupported data type: " + colTypes[i] + " for table " + tableName);
                                throw new RuntimeException("Unsupported data type: " + colTypes[i]);
                            }
                        } // end for (i <= colCount)
                        rowCount++;
                        ps.addBatch();
                        if ((rowCount % 1000 == 0) || (rs.isLast())) {

                            try {
                                ps.executeBatch();
                                targetConn.commit();
                                ps.clearBatch();
                            } catch (MySQLTransactionRollbackException e) { System.err.println("Rollback Transaction Happen!!!"); }
                            catch (MySQLIntegrityConstraintViolationException e) { System.err.println("MySQLIntegrityConstraintViolationException: Duplicate entry OR deadlock found!!!"); }
                            catch (BatchUpdateException e) { System.err.println("BatchUpdateException: Duplicate entry OR deadlock found!!!"); }
                            catch (CommunicationsException e) {
                                TransJobService.logger.warn(Thread.currentThread().getName() + ": target connection failure, try to reconnect");
                                targetConn = DriverManager.getConnection(transJob.getDestUrl(), transJob.getDestUser(), transJob.getDestPassword());

                                continue;
                            }

                            TransJobService.atomicCount.get(tableIndex).addAndGet(rowCount);
                            totalRowCount += rowCount;
                            rowCount = 0;
                        }
                    } // end of while(rs.next())
                    ps.close();
                    long endTime = System.currentTimeMillis();
                    TransJobService.logger.info(Thread.currentThread().getName() + ": " + fetchSql + " : TRANS " + totalRowCount + " rows, time cost: " +
                            (endTime - beginTime)/1000);
                }

                rs.close();
                break;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } // end loop for (retry)
    }

    public static int getIntDayFromStart(int start, int offset) {
        int day = -1;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Calendar cal = Calendar.getInstance();
            cal.setTime(sdf.parse(start + ""));
            cal.add(Calendar.DAY_OF_MONTH, offset);
            day = cal.get(Calendar.YEAR) * 10000 + (cal.get(Calendar.MONTH)+1) * 100 + cal.get(Calendar.DAY_OF_MONTH);
        } catch (ParseException e) { e.printStackTrace(); }
        return day;
    }

    public void doOneTableTransJob() {
        String fetchSql;
        String tableName = tableDef.getName();
        String colList = "";
        int colCount = tableDef.getColumns().size();

        for (int i = 0; i < colCount; i++) {
            colList += tableDef.getColumns().get(i);
            if (i < (colCount - 1)) {
                colList += ", ";
            }
        }
        String targetSql = "INSERT INTO " + tableName + " (" + colList + ") " + "VALUES (";
        for (int i = 1; i <= colCount; i++) {
            targetSql += "?";
            if (i != colCount) {
                targetSql += ", ";
            }
        }
        targetSql += ")";
        try {
            this.targetConn = DriverManager.getConnection(transJob.getDestUrl(), transJob.getDestUser(), transJob.getDestPassword());

            if (tableDef.getSplitType() == 0) {
                fetchSql = getRunSql();
                //System.out.println("Fetch data SQL: " + fetchSql);
                transData(fetchSql, targetSql);
            } else if (tableDef.getSplitType() == 1) {
                StringBuilder sb = new StringBuilder(rawSql);
                if (hasCondition) {
                    sb.append(" AND ");
                } else {
                    sb.append(" WHERE ");
                }

                if (min == max) {
                    sb.append(tableDef.getJobSplit() + " = " + min);
                    fetchSql = sb.toString();
                    transData(fetchSql, targetSql);
                } else {
                    int currentDay = (int)min;
                    while (currentDay <= (int)max) {
                        fetchSql = sb.toString() + tableDef.getJobSplit() + " = " + currentDay;

                        //System.out.println(fetchSql);

                        currentDay = getIntDayFromStart(currentDay, 1);

                        transData(fetchSql, targetSql);
                        if (targetConn.isClosed()) {
                            TransJobService.logger.warn(Thread.currentThread().getName() + "Dest connection closed by server!");
                            targetConn = DriverManager.getConnection(transJob.getDestUrl(), transJob.getDestUser(), transJob.getDestPassword());
                        }
                    }
                }
            }

            targetConn.close();
        } catch (SQLException e) { e.printStackTrace(); }
        finally {
            System.out.println(Thread.currentThread().getName() + " is done");
            TransJobService.countDownLatch.countDown();
        }
    }

    public void run() {

        TransJobService.logger.info(Thread.currentThread().getName() + " start transferring data for table " + tableDef.getName()
                + ", Min is: " + min + ", Max is: " + max + " ...\n");
        System.out.println("\n" + Thread.currentThread().getName() + " start transferring data for table " + tableDef.getName()
                + ", Min is: " + min + ", Max is: " + max + " ...\n");
        this.getRunSql();
        this.doOneTableTransJob();
    }
}
public class TransJobService {
    public static Logger logger = Logger.getLogger(TransJobService.class);
    public static CountDownLatch countDownLatch;
    public static ArrayList<AtomicLong> atomicCount;
    public static int reportInterval = 1;
    private String url;
    private String user;
    private String password;
    private ArrayList<TableDef> tableList;
    private TableTransJob transJob;
    private int workThdNum;
    private SimpleConPool srcConnPool;

    public TransJobService(TableTransJob transJob) {
        url = transJob.getSrcUrl();
        user = transJob.getSrcUser();
        password = transJob.getSrcPassword();
        tableList = transJob.getTableList();
        atomicCount = new ArrayList<AtomicLong>();
        for (int i = 0; i < tableList.size(); i++)
            atomicCount.add(new AtomicLong());

        workThdNum = 0;
        this.transJob = transJob;
        for (TableDef table : tableList) {
            workThdNum += table.getWorkerNum();
        }
        ConnectionHandler conHandler = new ConnectionHandler();
        srcConnPool = conHandler.createConPool(url, user, password, workThdNum+2);
        countDownLatch = new CountDownLatch(workThdNum);
        FileAppender appender = null;
        SimpleLayout layout = new SimpleLayout();
        try
        {
            //把输出端配置到out.txt
            appender = new FileAppender(layout,"data_transfer.log", true);
        }catch(IOException e) { e.printStackTrace(); }
        logger.addAppender(appender);
        logger.setLevel((Level) Level.DEBUG);
    }


    public static int getDaysBetween(int start, int end) {
        int day = -1;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            Calendar cal = Calendar.getInstance();
            cal.setTime(sdf.parse(start + ""));
            long time1 = cal.getTimeInMillis();
            cal.setTime(sdf.parse(end + ""));
            long time2 = cal.getTimeInMillis();
            long between_days = (time2 - time1)/(1000*3600*24);
            day = (int)between_days;
        } catch (ParseException e) { e.printStackTrace(); }
        return day;
    }

    // 获取范围
    private long[] getLimit(TableDef table) {
        long[] limit = new long[2];
        limit[0] = Long.MIN_VALUE;
        limit[1] = Long.MIN_VALUE;
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            String sql = "SELECT MIN(" + table.getJobSplit() + "), MAX(" + table.getJobSplit() + ") FROM " + table.getName();
            PreparedStatement ps;
            ps = (PreparedStatement)conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                long min = rs.getLong(1);
                if (rs.wasNull()) {
                    break;
                }
                long max = rs.getLong(2);
                if (rs.wasNull()) {
                    break;
                }
                limit[0] = min;
                limit[1] = max;

                System.out.println("Whole table " + table.getName() + ": Min is: " + min + ", Max is: " + max);
            }
            conn.close();
        } catch (SQLException e) { e.printStackTrace(); }

        if (limit[0] == Long.MIN_VALUE && limit[1] == Long.MIN_VALUE) {
            logger.warn("Fetch no result: select min, max for " + table.getName());
            throw new RuntimeException("Fetch no result: select min, max for " + table.getName());
        }
        return limit;
    }

    private void setColList(TableDef table) {
        if (table.getAllCol() == false)
            return;
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            String sql = "desc " + table.getName();
            PreparedStatement ps;
            ps = (PreparedStatement)conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ArrayList<String> colList = new ArrayList<String>();
            while(rs.next()) {
                colList.add(rs.getString(1));
                if (rs.wasNull()) {
                    logger.warn("rs cannot be null in setColList");
                    throw new RuntimeException("Cannot be NULL");
                }
            }
            table.setColumns(colList);
            //System.out.println("Column List:" + colList);
            conn.close();
        } catch (SQLException e) { e.printStackTrace(); }
    }

    public void jobDispatcher() {
        // ExecutorService service = Executors.newFixedThreadPool(workThdNum);
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {}
        long[] limit;
        int index = 0;
        for (TableDef table : tableList) {
            limit = getLimit(table);
            setColList(table);
            String splitCol = table.getJobSplit();
            boolean hasCond = false;
            long start = limit[0];
            long end = limit[1];
            long thdNum = table.getWorkerNum();
            long section = 0;
            if (table.getSplitType() == 0) {
                section = (end - start + 1) / thdNum;
            } else if (table.getSplitType() == 1) {
                section = (getDaysBetween((int)start, (int)end) + 1) / thdNum;
            }
            String rawSql = "SELECT ";

            if (table.getAllCol()) {
                rawSql += "*";
            } else {
                for (int i = 0; i < table.getColumns().size(); i++) {
                    rawSql += table.getColumns().get(i);
                    if (i < (table.getColumns().size() - 1)) {
                        rawSql+= ", ";
                    }
                }
            }

            rawSql += " FROM " + table.getName();

            if (table.getCondition() != null) {
                hasCond = true;
                rawSql += " WHERE " + "(" + table.getCondition() + ")";
            }

            try {
                int days = 0;
                if (table.getSplitType() == 1) {
                    days = (int) section;
                }
                for (int i = 1; i <= thdNum; i++) {
                    if (i == 1) {
                        long endPoint = 0;
                        if (table.getSplitType() == 0)
                            endPoint = start + section -1;
                        else if (table.getSplitType() == 1)
                            endPoint = SourceReader.getIntDayFromStart((int)start, days-1);

                        new Thread(new SourceReader(rawSql, hasCond, splitCol, start, endPoint, table, transJob,
                                srcConnPool, index)).start();
                    } else if (i == thdNum) {
                        long startPoint = 0;
                        if (table.getSplitType() == 0)
                            startPoint = start + (i - 1) * section;
                        else if (table.getSplitType() == 1)
                            startPoint = SourceReader.getIntDayFromStart((int)start, (int)((i - 1) * section));

                        new Thread(new SourceReader(rawSql, hasCond, splitCol, startPoint, end, table, transJob,
                                srcConnPool, index)).start();
                    } else {
                        long startPoint = 0;
                        long endPoint = 0;
                        if (table.getSplitType() == 0) {
                            startPoint = start + (i - 1) * section;
                            endPoint = start + i * section - 1;
                        } else if (table.getSplitType() == 1) {
                            startPoint = SourceReader.getIntDayFromStart((int) start, (int) ((i - 1) * section));
                            endPoint = SourceReader.getIntDayFromStart((int)start, (int)(i * section - 1));
                        }
                        new Thread(new SourceReader(rawSql, hasCond, splitCol, startPoint, endPoint, table, transJob,
                                srcConnPool, index)).start();
                    }
                }
            } catch (RuntimeException e) { e.printStackTrace(); }
            index++;
        }

        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
        try {
            long beginTime = System.currentTimeMillis();
            long[] midCount = new long[tableList.size()];
            Thread.sleep(1000);
            while (countDownLatch.getCount() > 0) {
                for (int i = 0; i < tableList.size(); i++) {
                    midCount[i] = TransJobService.atomicCount.get(i).get();
                    System.out.println(df.format(new Date())
                            + " finished records for table " + tableList.get(i).getName() + ": " + midCount[i]);
                }
                Thread.sleep(1000 * reportInterval);
            }
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.debug("Program abort by user!");
            System.err.println("Program abort by user!"); }

        long[] finalCount = new long[tableList.size()];
        for (int i = 0; i < tableList.size(); i++) {
            finalCount[i] = TransJobService.atomicCount.get(i).get();
            System.out.println(df.format(new Date()) + " total rows in " + tableList.get(i).getName() + ": " + finalCount[i]);
        }
        System.out.println("All job is done!");
    }

//    public static void main(String[] args) {
//        TableReader tableReader = TableReader.getInstance();
//        TableTransJob transJob = tableReader.getTableTransJob();
//        new TransJobService(transJob).jobDispatcher();
//        System.out.println("End");
//    }
}
