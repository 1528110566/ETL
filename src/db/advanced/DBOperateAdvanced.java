package db.advanced;

import db.DBOperate;
import exception.db.FuncCallException;
import exception.db.StupidCallingException;
import exception.parameter.UnSupportedFunctionException;
import system.parameter.FunctionParameter;
import system.parameter.ProcedureParameter;

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author TZ
 * @date 2019/8/20 19:22
 * 数据库高级操作
 */
public interface DBOperateAdvanced extends DBOperate {
    /**
     * 调用存储过程
     *
     * @param procPackageName 存储过程所在的包名
     * @param procName        存储过程名
     * @param parameters      调用存储过程所需参数
     */
    CallableStatement callProcedure(String procPackageName, String procName, ProcedureParameter... parameters) throws SQLException, UnSupportedFunctionException;

    /**
     * 调用函数，其中第一个参数必须是out类型，用作返回值
     *
     * @param funcPackageName 函数所在的包名
     * @param funcName        函数名
     * @param parameters      调用函数所需参数，其中第一个参数类型为out
     */
    CallableStatement callFunction(String funcPackageName, String funcName, FunctionParameter... parameters) throws SQLException, UnSupportedFunctionException, FuncCallException;

    /**
     * 从源表插入数据，拼接sql
     * 调用时应当保证两个数组的内容是一一对应的
     *
     * @param sourceTableName 源表表名
     * @param targetTableName 目标表表名
     * @param sourceColumns   源表列名
     * @param targetColumns   目标表列名
     * @param whereCondition  where条件语句
     * @return 返回受影响的行数
     */
    int insertFromSourceTable(String sourceTableName, String targetTableName, String[] sourceColumns, String[] targetColumns, String whereCondition) throws StupidCallingException;

    /**
     * 将已有数据插入到目标表，拼接sql
     * 调用时应当保证两个数组的内容是一一对应的
     *
     * @param targetTableName 目标表表名
     * @param sourceDate      已有数据
     * @param targetColumns   目标表列名
     * @return 返回受影响的行数
     */
    int insertFromSourceDate(String targetTableName, Object[] sourceDate, String[] targetColumns) throws StupidCallingException;
}
