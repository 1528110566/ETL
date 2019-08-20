package exception.parameter;

/**
 * @author TZ
 * @date 2019/8/20 16:49
 * 用于使用存储过程时参数的相关错误
 */
public class ParameterException extends Exception {
    public ParameterException() {
    }

    public ParameterException(String message) {
        super(message);
    }
}
