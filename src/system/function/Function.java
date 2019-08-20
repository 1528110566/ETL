package system.function;

import exception.function.FunctionArgumentLengthException;
import exception.function.FunctionArgumentTypeException;
import exception.function.FunctionException;
import oracle.sql.DATE;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author TZ
 * @date 2019/8/14 10:06
 * 实现Oracle中的一些系统级函数
 */
public class Function {
    /**
     * 实现Oracle中的decode函数，此处不支持char或者Character类型的参数，统一使用String，否则会报FunctionArgumentTypeException；
     * 参数个数必须>=2，否则报FunctionArgumentLengthException。
     */
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

    /**
     * 实现Oracle中的nvl函数
     */
    public static <T> T nvl(T t1, T t2) {
        if (t1 == null) {
            return t2;
        }
        return t1;
    }

    /**
     * 获得系统时间
     */
    public static DATE sysdate() throws SQLException {
        return new DATE(new Date());
    }

    /**
     * 实现Oracle的add_months，i为正数时加月份，i为负数时减月份
     */
    public static Date add_months(Date date, int i) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, i);
        return calendar.getTime();
    }

    /**
     * 如果不输入格式，则默认为"yyyy-MM-dd"
     */
    public static Date to_date(String str) throws ParseException {
        // 默认指定为年-月-日，Oracle中不指定则报错
        return to_date(str, "yyyy-MM-dd");
    }

    /**
     * reg是Oracle的格式，方法体内会尝试转换成Java的格式
     *
     * @param str 待转换字符串
     * @param reg 转换匹配格式
     */
    public static Date to_date(String str, String reg) throws ParseException {
        String temp = reg;
        // 适配Oracle中的24小时制
        if (reg.contains("hh24")) {
            reg = reg.replaceAll("hh24", "HH");
        }
        // 适配Oracle中的“月”
        if (reg.contains("mm")) {
            reg = reg.replaceAll("mm", "MM");
        }
        // 适配Oracle中的“分”
        if (reg.contains("mi")) {
            reg = reg.replaceAll("mi", "mm");
        }
        // 调试用
        //System.out.println(reg);
        try {
            SimpleDateFormat format = new SimpleDateFormat(reg);
            return format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new ParseException("传入的reg为" + temp + "，转换后的reg为" + reg, 106);
        }
    }

    /**
     * 实现Oracle中的replace函数
     *
     * @param str         如果该参数为空，直接返回null
     * @param searching   如果该参数为空，返回str
     * @param replacement 如果该参数为null，认为是""
     */
    public static String replace(String str, String searching, String replacement) {
        if (str == null) {
            return null;
        }
        if (searching == null) {
            return str;
        }
        if (replacement == null) {
            return str.replaceAll(searching, "");
        }
        return str.replaceAll(searching, replacement);
    }

    /**
     * 如上，replacement参数缺省，默认为""
     */
    public static String replace(String str, String searching) {
        return replace(str, searching, "");
    }

    /**
     * 实现Oracle的concat函数
     */
    private static String concat(String str1, String str2) {
        if (str1 == null && str2 == null) {
            return null;
        }
        if (str1 == null) {
            return str2;
        }
        if (str2 == null) {
            return str1;
        }
        return str1 + str2;
    }

    /**
     * 实现Oracle的concat函数，传入的参数只能是Integer、Character或者String中的某一个，
     * 否则会报FunctionArgumentTypeException错误。
     * 【注意】：Oracle中浮点型转为字符型的规则和Java的规则不同，如Oracle将0.1转为".1"，但是Java转成"0.1"，
     * 使用String型传入即可
     */
    public static String concat(Object obj1, Object obj2) throws FunctionException {
        String errorMessage = "传入的参数类型应该为Integer、Character或者String中的某一个";
        if (obj1 == null && obj2 == null) {
            return null;
        }
        String str1;
        String str2;
        if (obj1 == null) {
            str1 = null;
        } else {
            if (obj1 instanceof Character || obj1 instanceof String
                    || obj1 instanceof Integer) {
                str1 = String.valueOf(obj1);
            } else {
                throw new FunctionArgumentTypeException(errorMessage);
            }
        }
        if (obj2 == null) {
            str2 = null;
        } else {
            if (obj2 instanceof Character || obj2 instanceof String
                    || obj2 instanceof Integer) {
                str2 = String.valueOf(obj2);
            } else {
                throw new FunctionArgumentTypeException(errorMessage);
            }
        }
        return concat(str1, str2);
    }
}
