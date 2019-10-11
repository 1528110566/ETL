package db;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author TZ
 * @date 2019/8/14 10:01
 */
public interface DBOperate {
    /**
     * 提交
     */
    void commit() throws SQLException;

    /**
     * 选择
     */
    ResultSet select(String sql);

    /**
     * 插入
     */
    int insert(String sql);

    /**
     * 删除
     */
    int delete(String sql);

    /**
     * 创建
     */
    int create(String sql);

    /**
     * 截断
     */
    int truncate(String sql);

    /**
     * 删除
     */
    int drop(String sql);


    /**
     * 适配Oracle的execute immediate
     *
     * @param sql 待执行的SQL语句
     */
    void execute(String sql) throws SQLException;
}
