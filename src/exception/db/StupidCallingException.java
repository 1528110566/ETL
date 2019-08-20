package exception.db;

/**
 * @author TZ
 * @date 2019/8/20 19:50
 */
public class StupidCallingException extends DBException {
    public StupidCallingException() {
    }

    public StupidCallingException(String message) {
        super(message);
    }
}
