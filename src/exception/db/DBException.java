package exception.db;

/**
 * @author TZ
 * @date 2019/8/20 15:58
 */
public class DBException extends Exception {
    public DBException() {
    }

    public DBException(String message) {
        super(message);
    }
}
