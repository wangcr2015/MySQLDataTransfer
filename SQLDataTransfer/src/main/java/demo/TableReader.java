package demo;

/**
 * Created by 006975 on 2017/1/19.
 * Table用jason的格式写成，分别有Table Name，要抽取的字段列表，分批任务的字段（默认使用主键，可以定义一个日期字段）
 * 如果表没有主键
 * jason文本格式如下
 {
 "tableList":
 [
 {
 "name" : "account",
 "condition" : null,
 "allCol" : false,
 "columns" : [ "id", "date", "outgo", "type", "amount", "comments"],
 "jobSplit" : "date",
 "workerNum" : 1
 }
 ]
 }
 */
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

class TableDef {
    private String name;
    private String condition = null;
    private boolean allCol = false;
//    private boolean exceptPK = false;
    private String jobSplit = "id";
    private ArrayList<String> columns;
    // 0 - primary key, 1 - date,like 20161212
    private int splitType = 0;
    private int workerNum = 1;

    public TableDef() { columns = new ArrayList<String>(); }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getCondition() { return condition; }
    public void setCondition(String cond) { this.condition = cond; }
    public String getJobSplit() { return jobSplit; }
    public void setJobSplit(String split) { this.jobSplit = split; }
    public ArrayList<String> getColumns() { return columns; }
    public void setColumns(ArrayList<String> columns) { this.columns = columns; }
    public boolean getAllCol() { return allCol; }
    public void setAllCol(boolean allCol) { this.allCol = allCol; }
    public void setSplitType(int value) { this.splitType = value; }
    public int getSplitType() { return splitType; }
    public int getWorkerNum() { return workerNum; }
    public void setWorkerNum(int value) { this.workerNum = value; }
//    public boolean getExceptPK() { return exceptPK; }
//    public void setExceptPK(boolean value) { this.exceptPK = value; }

    @Override
    public String toString() {
        return super.toString() + "\n" + "Name: " + name + "\ncondition: " + condition
                + "\nallCol: " + allCol + "\njobSplit: " + jobSplit + "\ncolumns: " + columns.toString()
                +  "\nsplitType: " + splitType + "\nworkerNum: " + workerNum + "\n";
    }
}
class TableTransJob {
    private String srcUrl;
    private String destUrl;
    private String srcUser;
    private String srcPassword;
    private String destUser;
    private String destPassword;
    private ArrayList<TableDef> tableList;
    public TableTransJob() { tableList = new ArrayList<TableDef>(); }

    public ArrayList<TableDef> getTableList() { return tableList; }
    public void setTableList(ArrayList<TableDef> tableList) { this.tableList = tableList; }
    public String getSrcUrl() { return srcUrl; }
    public void setSrcUrl(String value) { this.srcUrl = value; }
    public String getSrcUser() { return srcUser; }
    public void setSrcUser(String value) { this.srcUser = value; }
    public String getSrcPassword() { return srcPassword; }
    public void setSrcPassword(String value) { this.srcPassword = value; }
    public String getDestUrl() { return destUrl; }
    public void setDestUrl(String value) { this.destUrl = value; }
    public String getDestUser() { return destUser; }
    public void setDestUser(String value) { this.destUser = value; }
    public String getDestPassword() { return destPassword; }
    public void setDestPassword(String value) { this.destPassword = value; }

    @Override
    public String toString() {
        String result = super.toString() + "\n";
        result += "\nsrcUrl: " + srcUrl + "\nsrcUser: " + srcUser + ", srcPasswd: " + srcPassword;
        result += "\ndestUrl: " + destUrl + "\ndestUser: " + destUser + ", destPasswd: " + destPassword;
        result += "\n\n";
        for (TableDef tableDef : tableList) {
            result += tableDef.toString();
        }

        return result;
    }
}

public class TableReader {

    private TableTransJob tableTransJob;
    private static TableReader instance;
    private TableReader() {
        try {
            //read json file data to String
            byte[] jsonData = Files.readAllBytes(Paths.get("./src/main/resources/tables.json"));

            //create ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            //convert json string to object
            tableTransJob = objectMapper.readValue(jsonData, TableTransJob.class);
        } catch (IOException e) {}
    }

    private TableReader(String jsonFile) {
        try {
            //read json file data to String
            byte[] jsonData = Files.readAllBytes(Paths.get(jsonFile));

            //create ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            //convert json string to object
            tableTransJob = objectMapper.readValue(jsonData, TableTransJob.class);
        } catch (IOException e) { e.printStackTrace(); }
    }

    public static TableReader getInstance() {
        instance = new TableReader();
        return instance;
    }
    public static TableReader getInstance(String jsonFile) {
        instance = new TableReader(jsonFile);
        return instance;
    }

    public TableTransJob getTableTransJob() { return instance.tableTransJob; }

    /*
    public static void main(String[] args) throws IOException {
        //read json file data to String
        byte[] jsonData = Files.readAllBytes(Paths.get("./src/main/resources/tables.json"));

        //create ObjectMapper instance
        ObjectMapper objectMapper = new ObjectMapper();

        //convert json string to object
        TableTransJob tableDefList = objectMapper.readValue(jsonData, TableTransJob.class);

        System.out.println("TableTransJob Object\n" + tableDefList);

        //convert Object to json string
        TableTransJob tableJob = new TableTransJob();
        ArrayList<String> cols = new ArrayList<String>();
        cols.add("id");
        cols.add("name");
        TableDef tableDef = new TableDef();
        tableDef.setColumns(cols);
        tableDef.setName("mytab");
        ArrayList<TableDef> tableList = new ArrayList<TableDef>();
        tableList.add(tableDef);
        tableJob.setTableList(tableList);
        //configure Object mapper for pretty print
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);

        //writing to console, can write to any output stream such as file
        StringWriter stringTableList = new StringWriter();
        objectMapper.writeValue(stringTableList, tableJob);
        System.out.println("Table List JSON is\n"+stringTableList);
    }
    */
}
