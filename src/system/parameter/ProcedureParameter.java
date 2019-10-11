package system.parameter;

import exception.parameter.ParameterUseErrorException;
import exception.parameter.UnSupportedFunctionException;

import java.sql.SQLType;

/**
 * @author TZ
 * @date 2019/8/20 16:44
 * 用于存储过程的输入、输出参数
 */
public class ProcedureParameter {
    /**
     * 输入、输出判断标准，true为in，false为out
     */
    private boolean in;
    /**
     * 输出参数的类型，in为false时必输
     */
    private SQLType types;
    /**
     * 输入参数的内容，in为true时必输
     */
    private Object value;

    public ProcedureParameter(boolean in, SQLType types) throws ParameterUseErrorException {
        this.in = in;
        if (in && types != null) {
            throw new ParameterUseErrorException("当参数为类型为in时，应该调用ProcedureParameter(boolean, String)这个构造方法");
        }
        this.types = types;
    }

    public ProcedureParameter(boolean in, Object value) throws ParameterUseErrorException {
        this.in = in;
        if (!in) {
            throw new ParameterUseErrorException("当参数为类型为out时，应该调用ProcedureParameter(boolean, Types)这个构造方法");
        }
        this.value = value;
    }

    public boolean isIn() {
        return in;
    }

    public SQLType getTypes() throws UnSupportedFunctionException {
        if (in) {
            throw new UnSupportedFunctionException("参数为in类型，不支持获取其类型");
        }
        return types;
    }

    public Object getValue() throws UnSupportedFunctionException {
        if (!in) {
            throw new UnSupportedFunctionException("参数为out类型，不支持获取其内容");
        }
        return value;
    }
}
