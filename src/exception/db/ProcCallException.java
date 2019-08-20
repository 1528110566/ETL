package exception.db;

/**
 * @author TZ
 * @date 2019/8/20 15:59
 * 用于调用存储过程
 */
public class ProcCallException extends DBException {
    public ProcCallException() {
    }

    public ProcCallException(String message) {
        super(message);
    }
}
