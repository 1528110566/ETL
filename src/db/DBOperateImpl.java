package db;

import java.sql.ResultSet;
import java.sql.SQLException;

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
    public void commit() throws SQLException {
        dbUtil.commit();
    }

    @Override
    public ResultSet select(String sql) {
        return dbUtil.getResult(sql);
    }

    @Override
    public int insert(String sql) {
        return dbUtil.sendSQL(sql);
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

    @Override
    public void execute(String sql) throws SQLException {
        dbUtil.execute(sql);
    }
}
