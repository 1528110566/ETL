package db;

import java.sql.CallableStatement;
import java.sql.ResultSet;

/**
 * @author TZ
 * @date 2019/8/14 10:01
 */
public interface DBOperate {
    /**
     * 调用存储过程
     *
     * @param procName   存储过程名
     * @param parameters 调用存储过程所需参数
     */
    <T> CallableStatement callProcedure(String procName, T... parameters);

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
}
