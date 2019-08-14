package exception.tool;

/**
 * @author TZ
 * @date 2019/8/14 11:19
 * 工具使用时参数类型错误
 */
public class ToolArgumentTypeException extends ToolException{
    public ToolArgumentTypeException(String reason) {
        super(reason);
    }
}
