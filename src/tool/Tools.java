package tool;

import db.DBOperate;
import db.DBOperateImpl;
import exception.tool.ToolArgumentTypeException;
import exception.tool.ToolException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @author TZ
 * @date 2019/8/14 10:13
 */
public class Tools {
    private static DBOperate dbOperate = new DBOperateImpl();

    /**
     * 传入的两个参数是否相等
     * Oracle在decode函数中会隐式地将都是数字字符的字符串转换成int型，
     * 如decode(1,'1',2,3)返回的是2
     * 注意，使用该函数只能传入基本数据类型但不包括char，将char改为String
     */
    public static <T> boolean equals(T t1, T t2) throws ToolException {
        if (t1 instanceof Character || t2 instanceof Character) {
            throw new ToolArgumentTypeException("不能使用char或者Character，应该改为String");
        }
        Integer i1 = null, i2 = null;
        if (t1 instanceof String) {
            String s = t1.toString();
            if (isAllDigit(s)) {
                i1 = Integer.parseInt(s);
            }
        }
        if (t2 instanceof String) {
            String s = t2.toString();
            if (isAllDigit(s)) {
                i2 = Integer.parseInt(s);
            }
        }
        boolean eq1 = Objects.equals(t1, t2);
        boolean eq2 = Objects.equals(i1, i2);
        boolean eq3 = Objects.equals(t1, i2);
        boolean eq4 = Objects.equals(i1, t2);
        if (!eq1 && !eq3 && !eq4 && eq2) {
            return false;
        }
        return eq1 || eq2 || eq3 || eq4;
    }

    /**
     * 判断传入的字符串是否都是数字字符
     */
    private static boolean isAllDigit(String s) {
        for (char c : s.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }

    public static int getSequenceNextValue(String sequenceName) throws SQLException {
        String sql = "SELECT " + sequenceName + ".NEXTVAL FROM DUAL";
        System.out.println(sql);
        ResultSet resultSet = dbOperate.select(sql);
        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            return 0;
        }
    }
}
