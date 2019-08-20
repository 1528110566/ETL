package exception.parameter;

/**
 * @author TZ
 * @date 2019/8/20 16:49
 * 用户使用存储过程时传入错误的参数
 */
public class ParameterUseErrorException extends ParameterException {
    public ParameterUseErrorException() {
    }

    public ParameterUseErrorException(String message) {
        super(message);
    }
}
