package exception.function;

/**
 * @author TZ
 * @date 2019/8/14 10:31
 */
public class FunctionArgumentLengthException extends FunctionArgumentException {
    public FunctionArgumentLengthException() {
        super();
    }

    public FunctionArgumentLengthException(String message) {
        super(message);
    }
}
