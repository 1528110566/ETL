package exception.connection;

/**
 * @author TZ
 * 处理数据库连接异常
 */
public class ConnectException extends Exception {
    public ConnectException(String reason) {
        super(reason);
    }
}
