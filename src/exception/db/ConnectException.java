package exception.db;

/**
 * @author TZ
 * 处理数据库连接异常
 */
public class ConnectException extends DBException {
    public ConnectException(String reason) {
        super(reason);
    }
}
