package db;

import java.sql.CallableStatement;
import java.sql.ResultSet;

public class DBOperateImpl implements DBOperate {
    private static DBUtil dbUtil;

    static {
        try {
            dbUtil = DBUtil.getInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public <T> CallableStatement callProcedure(String procName, T... parameters) {
        return null;
    }

    @Override
    public ResultSet select(String sql) {
        return dbUtil.getResult(sql);
    }

    @Override
    public int insert(String sql) {
        return 0;
    }

    @Override
    public int delete(String sql) {
        return 0;
    }

    @Override
    public int create(String sql) {
        return 0;
    }

    @Override
    public int truncate(String sql) {
        return 0;
    }

    @Override
    public int drop(String sql) {
        return 0;
    }
}
