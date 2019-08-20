package exception.parameter;

/**
 * @author TZ
 * @date 2019/8/20 16:55
 */
public class UnSupportedFunctionException extends ParameterException {
    public UnSupportedFunctionException() {
    }

    public UnSupportedFunctionException(String message) {
        super(message);
    }
}
