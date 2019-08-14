import oracle.jdbc.driver.OracleDriver;

import java.sql.*;
import java.util.Properties;

public class test {
    public static void main(String[] args) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        Long time = System.nanoTime();
        try {

            Driver driver = new OracleDriver();
            DriverManager.deregisterDriver(driver);

            Properties pro = new Properties();
            pro.put("user", "sjjc_gzh");
            pro.put("password", "sjjc_gzh");
            connection = driver.connect("jdbc:oracle:thin:@10.199.138.34:1521:sjck", pro);
            //检测是否连接成功
            System.out.println(connection);

            PreparedStatement preparedStatement = connection.prepareStatement("select 1+1 from dual");
            System.out.println(preparedStatement.execute());
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println(resultSet.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println((System.nanoTime() - time) / Math.pow(10, 9) + "s");

    }
}
