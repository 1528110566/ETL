package db;

/**
 * @author TZ
 * @date 2019/8/14 10:01
 */
public interface DBOperate {
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
