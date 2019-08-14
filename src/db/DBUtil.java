package db;

import exception.connection.ConnectException;
import oracle.jdbc.driver.OracleDriver;
import system.constant.Constant;

import java.sql.*;
import java.util.Properties;

import static system.constant.Constant.*;

/**
 * @author TZ
 * @date 2019/8/14 10:02
 */
public class DBUtil {
    private static Connection connection;
    private static DBUtil dbUtil;

    public static DBUtil getInstance() throws Exception {
        if (dbUtil == null) {
            dbUtil = new DBUtil();
            connect();
            if (connect() == CONNECTION_ERROR) {
                throw new ConnectException(CONNECTION_ERROR.getMessage());
            }
        }
        return dbUtil;
    }

    private DBUtil() {

    }

    private static Constant connect() {
        try {
            Driver driver = new OracleDriver();
            DriverManager.deregisterDriver(driver);
            Properties pro = new Properties();
            pro.put("user", "sjjc_gzh");
            pro.put("password", "sjjc_gzh");
            connection = driver.connect("jdbc:oracle:thin:@10.199.138.34:1521:sjck", pro);
            if (connection == null) {
                return CONNECTION_ERROR;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return CONNECTION_SUCCESS;
    }

    private Constant sendSQL(String sql) {
        try {
            if (sql.trim().endsWith(";")) {
                sql = sql.trim().substring(0, sql.length() - 1);
            }
            PreparedStatement statement = connection.prepareStatement(sql);
            if (statement.execute()) {
                return SQL_SEND_SUCCESS;
            } else {
                return SQL_RUN_ERROR;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return SQL_SEND_ERROR;
        }
    }

    private ResultSet getResult(String sql) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            return statement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
