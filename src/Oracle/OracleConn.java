package Oracle;

import java.sql.Connection;
import java.sql.DriverManager;

public class OracleConn {
    private static Connection conn = null;
    boolean isinit = false;

    OracleConn() {

    }

    OracleConn(String url, String user, String password) {
        try {
            Class.forName("org.apache.hive.jdbc.OraclesDriver");
            conn = DriverManager.getConnection(url, user, password);
            isinit = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
