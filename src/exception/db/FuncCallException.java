package exception.db;

/**
 * @author TZ
 * @date 2019/8/20 18:49
 * 用于调用函数
 */
public class FuncCallException extends DBException {
    public FuncCallException() {
    }

    public FuncCallException(String message) {
        super(message);
    }
}
