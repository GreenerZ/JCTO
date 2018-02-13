package Hive;

import java.sql.*;

public class HiveConn {

    private static Connection conn = null;
    private static ResultSet rs = null;
    boolean isinit = false;

    public HiveConn() {

    }

    public HiveConn(String url) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection(url, "", "");
            System.out.println("Hive建立连接");
            isinit = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public ResultSet SQL(String sql) {
        try {
            PreparedStatement stat = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stat.execute("set hive.optimize.index.filter=false");
            stat.execute("set spark.default.parallelism=120");
            stat.setFetchSize(1000);
            stat.setFetchDirection(1000);
            System.out.println("开始查询");
            rs = stat.executeQuery();
            System.out.println("查询结束");
            return rs;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
    }
}
