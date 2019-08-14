package system.function;

import exception.function.FunctionArgumentLengthException;
import exception.function.FunctionArgumentTypeException;
import exception.function.FunctionException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author TZ
 * @date 2019/8/14 10:06
 * 实现Oracle中的一些系统级函数
 */
public class Function {
    @SafeVarargs
    public static <T> T decode(T... t) throws FunctionException {
        if (t.length < 2) {
            throw new FunctionArgumentLengthException("函数参数的个数不正确");
        }
        for (T value : t) {
            if (value instanceof Character) {
                throw new FunctionArgumentTypeException("函数参数类型不正确，不能使用char或者Character，应该改为String");
            }
        }
        ArrayList<T> arrayList = new ArrayList<>(t.length);
        arrayList.addAll(Arrays.asList(t));
        T firstValue = arrayList.get(0);
        for (int i = 1; i < arrayList.size(); ) {
            if (Objects.equals(firstValue, arrayList.get(i))) {
                return arrayList.get(i + 1);
            }
            i += 2;
        }
        if (t.length % 2 == 0) {
            return arrayList.get(arrayList.size() - 1);
        }
        return null;
    }
}
