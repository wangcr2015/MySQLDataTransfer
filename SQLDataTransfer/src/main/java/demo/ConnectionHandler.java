package demo;

import java.sql.SQLException;

public class ConnectionHandler {
    public static String driver = "com.mysql.jdbc.Driver";

    public static String url = "jdbc:mysql://127.0.0.1:3306/test";
    public static String user = "root";
    public static String passwd = "root";
    private SimpleConPool conPool = null;

    public SimpleConPool createConPool(int threadcnt) {
        try {
            if (conPool == null) {
                // load driver
                Class.forName(driver);
                conPool = new SimpleConPool(url, user, passwd, threadcnt);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conPool;
    }

    public SimpleConPool createConPool(String urlAddr, String usr, String password, int threadNum) {
        try {
            if (conPool == null) {
                // load driver
                Class.forName(driver);
                conPool = new SimpleConPool(urlAddr, usr, password, threadNum);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conPool;
    }
}
