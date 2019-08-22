package db;

import exception.db.ConnectException;
import oracle.jdbc.driver.OracleDriver;
import system.constant.Constant;

import java.sql.*;
import java.util.Properties;

import static system.constant.Constant.CONNECTION_ERROR;
import static system.constant.Constant.CONNECTION_SUCCESS;
import static system.constant.Main.*;

/**
 * @author TZ
 * @date 2019/8/14 10:02
 */
public class DBUtil {
    private static Connection connection;
    private static DBUtil dbUtil;

    public static DBUtil getInstance() throws ConnectException, SQLException {
        if (dbUtil == null) {
            dbUtil = new DBUtil();
            if (connect() == CONNECTION_ERROR) {
                throw new ConnectException(CONNECTION_ERROR.getMessage());
            }
        }
        return dbUtil;
    }

    private DBUtil() {

    }

    private static Constant connect() throws SQLException {
        try {
            Driver driver = new OracleDriver();
            DriverManager.deregisterDriver(driver);
            Properties pro = new Properties();
//            pro.put("user", "sjjc_gzh");
//            pro.put("password", "sjjc_gzh");
//            connection = driver.connect("jdbc:oracle:thin:@10.199.138.34:1521:sjck", pro);
            pro.put("user", USER);
            pro.put("password", PASSWORD);
            connection = driver.connect(URL, pro);
            if (connection == null) {
                return CONNECTION_ERROR;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("数据库连接成功\t" + URL + "\t" + USER);
        connection.setAutoCommit(false);
        return CONNECTION_SUCCESS;
    }

    public int sendSQL(String sql) {
        try {
            if (sql.trim().endsWith(";")) {
                sql = sql.trim().substring(0, sql.length() - 1);
            }
            // TODO
            System.out.println("发送SQL:" + sql);
            PreparedStatement statement = connection.prepareStatement(sql);
            return statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public ResultSet getResult(String sql) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            return statement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public Connection getConnection() {
        return connection;
    }

    void commit() throws SQLException {
        connection.commit();
    }
}
